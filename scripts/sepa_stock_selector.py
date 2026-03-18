#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SEPA (Strategy with Pure Price Pattern) 选股脚本

实现马克·米勒维尼的SEPA策略+VCP形态条件选股

筛选条件：
1. 剔除ST股票和上市不满1年的次新股
2. 最近一季度营业收入同比增长率>25%
3. 最近一季度净利润同比增长率>30%且环比增长为正
4. 股价处于50日均线和150日均线之上
5. 最近10个交易日平均成交量大于120日均量(放量)
6. 净资产收益率(ROE)>15%
7. 最近三年净利润复合增长率>20%

数据源：Tushare Pro (需要配置 TUSHARE_TOKEN)
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import get_config


class SEPASelector:
    """SEPA策略选股器"""
    
    def __init__(self):
        """初始化Tushare API"""
        self._api = None
        self._init_tushare()
    
    def _init_tushare(self):
        """初始化Tushare API"""
        config = get_config()
        
        if not config.tushare_token:
            logger.error("TUSHARE_TOKEN 未配置，请检查 .env 文件")
            return
        
        try:
            import tushare as ts
            ts.set_token(config.tushare_token)
            self._api = ts.pro_api()
            logger.info("Tushare API 初始化成功")
        except Exception as e:
            logger.error(f"Tushare API 初始化失败: {e}")
    
    def is_available(self) -> bool:
        """检查数据源是否可用"""
        return self._api is not None
    
    def _check_rate_limit(self):
        """简单的速率限制检查"""
        import time
        if not hasattr(self, '_last_request_time'):
            self._last_request_time = 0
        
        # 每秒最多1次请求
        elapsed = time.time() - self._last_request_time
        if elapsed < 1.1:
            time.sleep(1.1 - elapsed)
        self._last_request_time = time.time()
    
    def get_stock_list(self) -> List[str]:
        """
        获取全部A股上市股票列表
        """
        if not self._api:
            return []
        
        try:
            self._check_rate_limit()
            df = self._api.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,list_date'
            )
            if df is not None and not df.empty:
                # 过滤ST股票和次新股
                df['code'] = df['ts_code'].apply(lambda x: x.split('.')[0])
                return df['code'].tolist()
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
        
        return []
    
    def get_stock_info(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        获取股票基本信息
        """
        if not self._api:
            return None
        
        try:
            self._check_rate_limit()
            ts_code = self._convert_code(stock_code)
            df = self._api.stock_basic(ts_code=ts_code, fields='ts_code,name,list_date,market,exchange')
            
            if df is not None and not df.empty:
                row = df.iloc[0]
                return {
                    'code': stock_code,
                    'name': row['name'],
                    'list_date': row['list_date'],
                    'market': row.get('market'),
                    'exchange': row.get('exchange'),
                }
        except Exception as e:
            logger.debug(f"获取股票信息失败 {stock_code}: {e}")
        
        return None
    
    def _convert_code(self, stock_code: str) -> str:
        """转换股票代码为Tushare格式"""
        code = stock_code.strip()
        if '.' in code:
            return code.upper()
        
        # 判断市场
        if code.startswith(('600', '601', '603', '688')):
            return f"{code}.SH"
        elif code.startswith(('000', '002', '300')):
            return f"{code}.SZ"
        elif code.startswith(('8', '4', '9')):  # 北交所
            return f"{code}.BJ"
        else:
            return f"{code}.SZ"
    
    def is_st_stock(self, stock_code: str) -> bool:
        """判断是否为ST股票"""
        info = self.get_stock_info(stock_code)
        if info and info.get('name'):
            return 'ST' in info['name'] or '*ST' in info['name']
        return False
    
    def is_new_stock(self, stock_code: str, min_days: int = 365) -> bool:
        """判断是否为次新股（上市不满min_days天）"""
        info = self.get_stock_info(stock_code)
        if not info or not info.get('list_date'):
            return True
        
        try:
            list_date = datetime.strptime(info['list_date'], '%Y%m%d')
            days_since_listing = (datetime.now() - list_date).days
            return days_since_listing < min_days
        except Exception:
            return True
    
    def get_financial_data(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        获取财务数据：营收同比、净利润同比和环比、ROE、净利润复合增长率
        
        Tushare免费版字段：
        - tr_yoy/or_yoy: 营业收入同比
        - netprofit_yoy: 净利润同比
        - q_op_qoq: 净利润环比（单季度）
        - roe: 净资产收益率
        """
        if not self._api:
            return None
        
        try:
            self._check_rate_limit()
            ts_code = self._convert_code(stock_code)
            
            # 获取财务指标 - 使用正确的字段名
            df = self._api.fina_indicator(
                ts_code=ts_code,
                fields='ts_code,end_date,tr_yoy,or_yoy,netprofit_yoy,q_op_qoq,roe'
            )
            
            if df is None or df.empty:
                return None
            
            # 按日期排序，取最新数据
            df = df.sort_values('end_date', ascending=False)
            
            # 去重，保留最新公告
            df = df.drop_duplicates(subset=['end_date'], keep='first')
            
            # 最近一季度数据
            latest = df.iloc[0]
            
            # 营收同比：优先用tr_yoy，其次or_yoy
            revenue_yoy = latest.get('tr_yoy') or latest.get('or_yoy') or 0
            revenue_yoy = float(revenue_yoy) if revenue_yoy is not None else 0
            
            # 净利润同比
            net_profit_yoy = float(latest.get('netprofit_yoy', 0) or 0)
            
            # 净利润环比
            net_profit_qoq = float(latest.get('q_op_qoq', 0) or 0)
            
            # ROE
            roe = float(latest.get('roe', 0) or 0)
            
            # 计算三年复合增长率
            cagr = 0
            if len(df) >= 4:
                # 取4年前同季度的数据
                four_years_ago_idx = min(3, len(df) - 1)
                four_years_ago = df.iloc[four_years_ago_idx]
                
                net_profit_current = latest.get('netprofit_yoy', 0)
                net_profit_4y_ago = four_years_ago.get('netprofit_yoy', 0)
                
                if net_profit_current and net_profit_4y_ago and net_profit_4y_ago > 0:
                    cagr = ((net_profit_current / net_profit_4y_ago) ** (1/3) - 1) * 100
            
            return {
                'revenue_yoy': revenue_yoy,  # 营收同比
                'net_profit_yoy': net_profit_yoy,  # 净利润同比
                'net_profit_qoq': net_profit_qoq,  # 净利润环比
                'roe': roe,  # ROE
                'cagr_3y': cagr,  # 三年复合增长率
            }
            
        except Exception as e:
            logger.debug(f"获取财务数据失败 {stock_code}: {e}")
            return None
    
    def get_price_and_ma(self, stock_code: str, days: int = 250) -> Optional[Dict[str, Any]]:
        """
        获取股价和均线数据
        
        需要获取足够的历史数据来计算150日均线
        """
        if not self._api:
            return None
        
        try:
            self._check_rate_limit()
            ts_code = self._convert_code(stock_code)
            
            end_date = datetime.now().strftime('%Y%m%d')
            # 增加历史数据获取天数，确保150日均线有数据
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
            
            df = self._api.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
            
            if df is None or df.empty:
                return None
            
            # 转换日期
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            df = df.sort_values('trade_date')
            
            # 计算均线
            df['ma5'] = df['close'].rolling(window=5).mean()
            df['ma10'] = df['close'].rolling(window=10).mean()
            df['ma20'] = df['close'].rolling(window=20).mean()
            df['ma50'] = df['close'].rolling(window=50).mean()
            df['ma150'] = df['close'].rolling(window=150).mean()
            df['ma200'] = df['close'].rolling(window=200).mean()
            
            # 计算成交量均线
            df['vol_ma10'] = df['vol'].rolling(window=10).mean()
            df['vol_ma120'] = df['vol'].rolling(window=120).mean()
            
            latest = df.iloc[-1]
            
            return {
                'close': float(latest['close']),
                'ma50': float(latest['ma50']) if pd.notna(latest['ma50']) else None,
                'ma150': float(latest['ma150']) if pd.notna(latest['ma150']) else None,
                'vol_ma10': float(latest['vol_ma10']) if pd.notna(latest['vol_ma10']) else None,
                'vol_ma120': float(latest['vol_ma120']) if pd.notna(latest['vol_ma120']) else None,
                'volume': float(latest['vol']),
            }
            
        except Exception as e:
            logger.debug(f"获取均线数据失败 {stock_code}: {e}")
            return None
    
    def check_sepa_conditions(self, stock_code: str) -> Dict[str, Any]:
        """
        检查股票是否满足SEPA选股条件
        
        Returns:
            dict: {
                'pass': bool,  # 是否通过所有条件
                'reasons': List[str],  # 通过/失败的原因
                'details': Dict[str, Any]  # 详细数据
            }
        """
        reasons = []
        details = {}
        
        # 1. 剔除ST股票
        if self.is_st_stock(stock_code):
            reasons.append(f"❌ {stock_code}: ST股票")
            return {'pass': False, 'reasons': reasons, 'details': details}
        details['st_check'] = True
        
        # 2. 剔除次新股（上市不满1年）
        if self.is_new_stock(stock_code):
            reasons.append(f"❌ {stock_code}: 上市不满1年的次新股")
            return {'pass': False, 'reasons': reasons, 'details': details}
        details['new_stock_check'] = True
        
        # 3. 财务数据检查
        financial = self.get_financial_data(stock_code)
        if not financial:
            reasons.append(f"⚠️ {stock_code}: 无法获取财务数据")
            return {'pass': False, 'reasons': reasons, 'details': details}
        
        details['financial'] = financial
        
        # 3a. 营业收入同比增长率>25%
        revenue_yoy = financial.get('revenue_yoy', 0)
        if revenue_yoy <= 25:
            reasons.append(f"❌ {stock_code}: 营收同比{revenue_yoy:.1f}% ≤ 25%")
            return {'pass': False, 'reasons': reasons, 'details': details}
        reasons.append(f"✅ 营收同比: {revenue_yoy:.1f}% > 25%")
        
        # 3b. 净利润同比增长率>30%且环比增长为正
        net_profit_yoy = financial.get('net_profit_yoy', 0)
        net_profit_qoq = financial.get('net_profit_qoq', 0)
        if net_profit_yoy <= 30 or net_profit_qoq <= 0:
            reasons.append(f"❌ {stock_code}: 净利润同比{net_profit_yoy:.1f}% ≤ 30% 或环比{net_profit_qoq:.1f}% ≤ 0%")
            return {'pass': False, 'reasons': reasons, 'details': details}
        reasons.append(f"✅ 净利润同比: {net_profit_yoy:.1f}% > 30%, 环比: {net_profit_qoq:.1f}% > 0%")
        
        # 6. ROE > 15%
        roe = financial.get('roe', 0)
        if roe <= 15:
            reasons.append(f"❌ {stock_code}: ROE {roe:.1f}% ≤ 15%")
            return {'pass': False, 'reasons': reasons, 'details': details}
        reasons.append(f"✅ ROE: {roe:.1f}% > 15%")
        
        # 7. 最近三年净利润复合增长率>20%
        cagr = financial.get('cagr_3y', 0)
        if cagr <= 20:
            reasons.append(f"❌ {stock_code}: 三年复合增长率{cagr:.1f}% ≤ 20%")
            return {'pass': False, 'reasons': reasons, 'details': details}
        reasons.append(f"✅ 三年复合增长率: {cagr:.1f}% > 20%")
        
        # 4. 股价处于50日均线和150日均线之上
        price_data = self.get_price_and_ma(stock_code)
        if not price_data:
            reasons.append(f"⚠️ {stock_code}: 无法获取价格数据")
            return {'pass': False, 'reasons': reasons, 'details': details}
        
        details['price'] = price_data
        
        close = price_data.get('close', 0)
        ma50 = price_data.get('ma50')
        ma150 = price_data.get('ma150')
        
        if ma50 is None or ma150 is None:
            reasons.append(f"⚠️ {stock_code}: 均线数据不足")
            return {'pass': False, 'reasons': reasons, 'details': details}
        
        if close <= ma50 or close <= ma150:
            reasons.append(f"❌ {stock_code}: 股价({close:.2f}) ≤ MA50({ma50:.2f}) 或 MA150({ma150:.2f})")
            return {'pass': False, 'reasons': reasons, 'details': details}
        reasons.append(f"✅ 股价({close:.2f}) > MA50({ma50:.2f}) > MA150({ma150:.2f})")
        
        # 5. 最近10个交易日平均成交量大于120日均量(放量)
        vol_ma10 = price_data.get('vol_ma10', 0)
        vol_ma120 = price_data.get('vol_ma120', 0)
        
        if vol_ma10 is None or vol_ma120 is None:
            reasons.append(f"⚠️ {stock_code}: 成交量数据不足")
            return {'pass': False, 'reasons': reasons, 'details': details}
        
        if vol_ma10 <= vol_ma120:
            reasons.append(f"❌ {stock_code}: 成交量不足，10日均量({vol_ma10:.0f}) ≤ 120日均量({vol_ma120:.0f})")
            return {'pass': False, 'reasons': reasons, 'details': details}
        reasons.append(f"✅ 放量: 10日均量({vol_ma10:.0f}) > 120日均量({vol_ma120:.0f})")
        
        # 全部通过
        reasons.insert(0, f"✅ {stock_code}: 通过SEPA筛选")
        return {'pass': True, 'reasons': reasons, 'details': details}
    
    def select_stocks(self, stock_list: List[str] = None, limit: int = 50) -> List[str]:
        """
        执行SEPA选股
        
        Args:
            stock_list: 股票代码列表，None时获取全部A股
            limit: 最大返回数量
            
        Returns:
            符合SEPA条件的股票代码列表
        """
        if not self.is_available():
            logger.error("Tushare API 不可用")
            return []
        
        # 获取股票列表
        if stock_list is None:
            logger.info("获取全部A股列表...")
            stock_list = self.get_stock_list()
        
        logger.info(f"开始筛选 {len(stock_list)} 只股票...")
        
        selected = []
        checked = 0
        
        for i, code in enumerate(stock_list):
            if (i + 1) % 100 == 0:
                logger.info(f"已检查 {i + 1}/{len(stock_list)} 只股票...")
            
            try:
                result = self.check_sepa_conditions(code)
                
                if result['pass']:
                    stock_info = self.get_stock_info(code)
                    name = stock_info['name'] if stock_info else 'Unknown'
                    selected.append(code)
                    logger.info(f"✅ 选中: {code} ({name})")
                    for reason in result['reasons']:
                        logger.info(f"   {reason}")
                    
                    if len(selected) >= limit:
                        logger.info(f"已达到选股数量上限: {limit}")
                        break
                        
            except Exception as e:
                logger.debug(f"检查 {code} 时出错: {e}")
                continue
            finally:
                checked += 1
        
        logger.info(f"\n选股完成: 检查了 {checked} 只股票，选中 {len(selected)} 只")
        
        return selected


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='SEPA策略选股')
    parser.add_argument('--limit', type=int, default=50, help='最大选股数量')
    parser.add_argument('--stocks', type=str, default=None, help='指定股票列表，逗号分隔')
    args = parser.parse_args()
    
    # 初始化选股器
    selector = SEPASelector()
    
    if not selector.is_available():
        logger.error("Tushare API 不可用，请检查配置")
        sys.exit(1)
    
    # 获取股票列表
    if args.stocks:
        stock_list = [s.strip() for s in args.stocks.split(',')]
        logger.info(f"使用指定的股票列表: {stock_list}")
    else:
        logger.info("获取全部A股...")
        stock_list = None
    
    # 执行选股
    selected = selector.select_stocks(stock_list, limit=args.limit)
    
    # 输出结果
    print("\n" + "=" * 50)
    print("SEPA选股结果")
    print("=" * 50)
    print(f"选中股票数量: {len(selected)}")
    print(f"股票代码: {','.join(selected)}")
    print("=" * 50)
    
    return selected


if __name__ == '__main__':
    main()
