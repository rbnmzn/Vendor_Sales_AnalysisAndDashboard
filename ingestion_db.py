import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time
logging.basicConfig(
    filename="logs/ingestion_db.logs",
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filemode="a"   
)
engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):  
    '''this function will injest dataframes into tables'''
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
def load_raw_data():    
    '''this function will load the CSV files and injest into the database'''
    start=time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            df=pd.read_csv('data/' + file)
            logging.info(f"Injesting {file} indb ")
            ingest_db(df, file[:-4], engine)
    
    end=time.time()
    total_time=(end - start)/60
    logging.info('Injesting Completed')    
    logging.info(f"Total time taken is {total_time} minutes")

if __name__=='__main__':
    load_raw_data()