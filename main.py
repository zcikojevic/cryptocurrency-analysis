"""
main.py
"""

import argparse
import threading
from datetime import datetime, timedelta
from sys import argv

import certifi
import numpy as np
import pandas as pd
import urllib3
from bs4 import BeautifulSoup

BASE_URL = 'https://coinmarketcap.com/'
MAX_THREADS = 10
FIRST_RUN_HELP = """ 1 - ensures all created files will have a header
 0 - appends the latest data to existing files """

def get_html_source(url):
    http = urllib3.PoolManager(
        cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

    response = http.request('GET', url)
    return response.data.decode('windows-1250')


def extract_name_supply():
    all_coins = 'coins/views/all/'

    soup = BeautifulSoup(get_html_source(BASE_URL + all_coins), 'lxml')

    table = soup.find('table', {'id': 'currencies-all'})
    rows = table.findAll('tr')

    data = 'Name;Symbol;Supply\n'

    for row in rows[1:]:
        symbol = row.find('span', {'class': 'currency-symbol'}).text.strip()
        name = row.find(
            'a', {'class': 'currency-name-container'})['href'].split('/')[2]
        supply = row.find(
            'td', {'class': 'circulating-supply'}).contents[1].text.strip()
        supply = supply if supply != '?' else 'NULL'
        data += name + ';' + symbol + ';' + supply + '\n'

    with open('../cryptocurrency_data/cryptocurrencies.csv', 'w') as output:
        output.write(data)

def worker(currencies, historical_data, first_run):
    for index, currency in currencies.iterrows():
        historical_data = historical_data.replace('COIN_NAME', currency['Name'])
        soup = BeautifulSoup(get_html_source(BASE_URL + historical_data), 'lxml')

        table = soup.find('table', {'class': 'table'})
        data = pd.read_html(str(table))[0]
        if data['Date'][0] == 'No data was found for the selected time period.':
            pass

        data['Date'] = pd.to_datetime(data.Date)
        data.to_csv(f'../cryptocurrency_data/historical_data/{currency["Name"]}.csv',
                    sep=';', index=False, header=first_run, mode='a')

def collect_historical_data(first_run, n_threads):
    now = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    historical_data = 'currencies/COIN_NAME/historical-data/?start=START&end=' + now

    if first_run:
        historical_data = historical_data.replace('START', '20130428')
    else:
        historical_data = historical_data.replace('START', now)

    currencies = pd.read_csv('../cryptocurrency_data/cryptocurrencies.csv', sep=';').head(11)

    threads = []
    worker_currencies = np.array_split(currencies, n_threads)

    for i in range(n_threads):
        t = threading.Thread(target=worker, args=(worker_currencies[i], historical_data, first_run))
        threads.append(t)
        t.start()

def parse_input_arguments():
    parser = argparse.ArgumentParser(description='Script that extracts cryptocurrency data from coinmarket',
                                    formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('first_run',
                        type=int,
                        metavar='first_run',
                        help=FIRST_RUN_HELP,
                        choices=set((1, 0)))

    parser.add_argument('--n_threads',
                        type=int,
                        choices=set(list(range(1, MAX_THREADS + 1))),
                        default=4)

    args = parser.parse_args()

    return args.first_run, args.n_threads

def main():
    first_run, n_threads = parse_input_arguments()
    extract_name_supply()
    collect_historical_data(first_run, n_threads)


if __name__ == '__main__':
    main()