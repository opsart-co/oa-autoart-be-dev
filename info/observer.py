# -*- coding: utf-8 -*-
import polars as pl
from collections import defaultdict

class EndpointInfoStrategy:
    def get_endpoint_info(self) -> defaultdict:
        return self.ep_info

class NumericalInfoMethods(EndpointInfoStrategy):
    def __init__(self, series:pl.Series):
        self.df_ep = series
        self.ep_info = defaultdict(lambda: {})

class StringInfoMethods(EndpointInfoStrategy):
    def __init__(self, series:pl.Series):
        self.df_ep = series
        self.ep_info = defaultdict(lambda: {})

class DataInfo:
    def __init__(self, dataframe:pl.DataFrame):
        self.df = dataframe
        self.info = defaultdict(lambda: {})
        self.analyze_endpoints()
    
    def set_endpoint_strategy(self, strategy:EndpointInfoStrategy) -> None:
        self._strategy = strategy
    
    def analyze_endpoints(self) -> None:
        for endpoint in self.df.columns:
            df_ep = self.df[endpoint]
            dtype = df_ep.dtype
            
            if isinstance(dtype, (pl.Int8, pl.Int16, pl.Int32, pl.Int64)):
                self.set_endpoint_strategy(strategy=NumericalInfoMethods(df_ep))
            elif isinstance(dtype, (pl.Utf8, pl.String, pl.Categorical)):
                self.set_endpoint_strategy(strategy=StringInfoMethods(df_ep))
            else:
                pass
            
            ep_info = self._strategy.get_endpoint_info()
            self.info[endpoint].update(ep_info)
    
    @property
    def get_info(self) -> dict:
        return dict(self.info)
