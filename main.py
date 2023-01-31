import pandas as pd
import json


class ColumnData():
    def __init__(self, json_file='test.json', json_data=None, user_request=None):
        self.json_file = json_file
        self.json_data = ColumnData.get_json_data(json_file)
        self.levels = ['ad', 
                       'adset', 
                       'campaign', 
                       'adaccount']

    def get_json_data(json_file):
        with open('test.json') as file:
            data = json.load(file)
        
        return data
    
    def set_user_request(self):
        user_request = input("Input user request: ").replace("'", "")
        self.user_request = user_request.split(', ')
        self.request_levels = []
        for request in self.user_request:
            print(request)
            table_requests = request.split('-')
            for table_request in table_requests:
                if table_request in self.levels and table_request not in self.request_levels:
                    self.request_levels.append(table_request)
                    print(self.request_levels)

        return self
    
    def set_priority_table(self):
        self.priority_tables = []
        self.request_tables = []
        for level in self.request_levels:
            self.priority_tables.append(self.levels.index(level))
        self.priority_tables.sort()
        for priority_level in self.priority_tables:
            self.request_tables.append(self.levels[priority_level])
        return self
    
    def generate_dataframe_table(self):
        dataframe_tables = {}
        for table in self.request_tables:
            dataframe_tables[table] = pd.DataFrame(self.json_data[table])
        self.dataframes = dataframe_tables
        return self
    
    def left_join_tables(self):
        for index, level in enumerate(self.dataframes):
            if index == 0:
                left_table_set = self.dataframes[level]
                if any(left_table_set):
                    right_index = self.request_tables[index + 1]
                    right_table_set = self.dataframes[right_index]
                    right_id = str(right_index + '_id')
                    left_table_set.merge(right_table_set, on=right_id, how='left')
        self.joined_tables = left_table_set.to_json()
        left_table_set.to_csv('test.csv')
        return


           
if __name__ == "__main__":
    data = ColumnData()
    data.set_user_request()
    data.set_priority_table()
    data.generate_dataframe_table()
    data.left_join_tables()
    # data.get_columns()