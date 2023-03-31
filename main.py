from __future__ import print_function

import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import requests
import pandas as pd
import json
from datetime import datetime

# If modifying these scopes, delete the file token.json.
# modificar tirando o .readonly do final
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
# escolher a planilha que quer conectar e um range só para conferir se conectou

token = "TOKEN_FILE_PATH_HERE"
credentials_2 = "CREDENTIALS_2_FILE_PATH_HERE"

"""Shows basic usage of the Sheets API.
Prints values from a sample spreadsheet.
"""
creds = None
# The file token.json stores the user's access and refresh tokens, and is
# created automatically when the authorization flow completes for the first
# time.
if os.path.exists(token):
    creds = Credentials.from_authorized_user_file(token, SCOPES)
# If there are no (valid) credentials available, let the user log in.
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            credentials_2, SCOPES)
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open(token, 'w') as token:
        token.write(creds.to_json())

service = build('sheets', 'v4', credentials=creds)

# Call the Sheets API
sheet = service.spreadsheets()

# --- STAKEHOLDERS DATAFRAME ---

#-> Vaults
SAMPLE_SPREADSHEET_ID = "SPREADSHEET_ID_HERE"
SAMPLE_RANGE_NAME = "SPREADSHEET_RANGE_NAME_HERE"

result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                            range=SAMPLE_RANGE_NAME).execute()
values = result.get('values', [])

vault_addresses = pd.DataFrame(values[1:], columns=values[0]).applymap(str)
vault_addresses = vault_addresses.loc[vault_addresses["Vault Address"] != "None"]
vault_addresses.rename(columns={"Vaults":"name","Vault Address":"address"}, inplace=True)
vault_addresses["type"] = "Vault"
vault_addresses["details"] = ""

#-> Partners
SAMPLE_SPREADSHEET_ID = "SPREADSHEET_ID_HERE"
SAMPLE_RANGE_NAME = "SPREADSHEET_RANGE_NAME_HERE"

result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                            range=SAMPLE_RANGE_NAME).execute()
values = result.get('values', [])

partners_addresses = pd.DataFrame(values[1:], columns=values[0]).applymap(str)
partners_addresses = partners_addresses.loc[partners_addresses["Wallet"] != "None"]
partners_addresses.rename(columns={"Partners":"name","Wallet":"address","Type":"details"}, inplace=True)
partners_addresses["type"] = "Partner"

stakeholders_df = pd.concat([vault_addresses[["name","type","details","address"]],partners_addresses[["name","type","details","address"]]], ignore_index=True)

requests.put(url="GOOGLE_FIREBASE_URL_HERE", data=json.dumps(stakeholders_df.to_dict("records")))

# --- GETTING API INFOS ---
network = "137"
address = "0x623EBdA5fc6B271DD597E20ae99927eA9eF8515e"
page_number = 0
page_size = 100

all_transactions = []

while True:
  url = f"https://api.covalenthq.com/v1/{network}/address/{address}/transactions_v2/?quote-currency=USD&format=JSON&block-signed-at-asc=false&no-logs=false&page-number={page_number}&page-size={page_size}&key=API_KEY_HERE"
  response = requests.get(url)
  results = response.json()
  r = results["data"]["items"]
  pagination = results["data"]["pagination"]["has_more"]

  all_transactions.extend(r)

  if pagination == True:
    page_number += 1
  else:
    break

df = pd.DataFrame(all_transactions)

df_mod = df.copy() #-> Backup of dataframe

# --- LOGS UNPACKING FUNCTION ---
def values_transaction(df):
  log_events_values = []
  
  for item in df:
    if item["decoded"] != None:
      if item["decoded"]["name"] != "Approval":
        if item["decoded"]["params"][-1]["name"] == "value" or item["decoded"]["params"][-1]["name"] == "amount":
          name,from_address,to_address,tx_hash,token_name, token_ticker, token_decimals, values = item["decoded"]["name"],item["decoded"]["params"][0]["value"],item["decoded"]["params"][1]["value"], item["tx_hash"], item["sender_name"], item["sender_contract_ticker_symbol"], item["sender_contract_decimals"], item["decoded"]["params"][-1]["value"]
          token_dict = {"name":name,
                        "from":from_address,
                        "to":to_address,
                        "tx_hash":tx_hash,
                        "token_name":token_name,
                         "token_ticker":token_ticker,
                         "token_decimals":token_decimals,
                         "values":values}

          log_events_values.append(token_dict)
          
  return log_events_values

df_mod["transfers_values"] = df_mod.log_events.map(values_transaction)

# --- MERGING AND CLEANING DATA ---
transfers_values_df = pd.DataFrame()

for item in df_mod.transfers_values:
  item_df = pd.DataFrame(item)
  transfers_values_df = pd.concat([transfers_values_df, item_df], ignore_index=True)

final_df = pd.merge(left=df_mod[["block_signed_at","tx_hash","from_address","to_address"]], right=transfers_values_df, on="tx_hash")

final_df.fillna(method="backfill", inplace=True)
# final_df.drop_duplicates(subset=["tx_hash","block_signed_at","values"],ignore_index=True,inplace=True)

final_df["values"] = final_df["values"].astype(float) / pow(10, final_df.token_decimals)
final_df.drop("token_decimals", axis=1, inplace=True)

final_df.block_signed_at = pd.to_datetime(final_df.block_signed_at, utc=True)
final_df["date"] = final_df.block_signed_at.dt.date

# Fixing problems
final_df.loc[(final_df.tx_hash == "0xab2d42132017475a38878a4ce541b9a90888b3f9a52b09af12bbfbbd09c935d5"),"to_address"] = "0xcf9908bef579833e9ded3306c42f33db50b22997"
final_df = final_df.query("token_name == 'DUX'").reset_index(drop=True)
final_df_mod = final_df.applymap(str)
final_df_mod["values"] = final_df_mod["values"].map(lambda x : x.replace(".",","))

requests.put(url="GOOGLE_FIREBASE_URL_HERE",
             data=json.dumps(final_df_mod.to_dict("records")))

# --- METRICS ---

# Token Price
#-> General Infos
url = "https://www.mexc.com/api/platform/spot/market/symbol?symbol=DUX_USDT"

response = requests.get(url)
results = response.json()
r = results["data"]

price, high_price_24h, low_price_24h, volume_24h_usd, volume_24h_usdt, percent_change_volume_25h = r["c"], r["h"], r["l"], r["q"], r["a"],r["percentChangeVolume24h"]

#-> K-lines Infos (Candlestick)
year = datetime.now().year
month = datetime.now().month
day = datetime.now().day

today_timestamp = str(int(datetime(year, month, day, 0, 0, 0, 0).timestamp())) + "000"

start_date = "1674518400000" #-> 2023-01-24
end_date = today_timestamp

url = f"https://www.mexc.com/api/platform/spot/market/kline?end={end_date}&interval=Day1&openPriceMode=LAST_CLOSE&start={start_date}&symbol=DUX_USDT"

response = requests.get(url)
# print(response.text)

results = response.json()
r = results["data"]

klines_df = pd.DataFrame(r)
klines_df.t = pd.to_datetime(klines_df.t,unit="s")

requests.put(url="GOOGLE_FIREBASE_URL_HERE",
             data=json.dumps(klines_df.applymap(str).applymap(lambda x : x.replace(".",",")).to_dict("records")))

# Staking
staking_df = final_df.query("name == 'Staked'").reset_index(drop=True)
tvl_usd = price * staking_df["values"].sum()
tvl_dux = staking_df["values"].sum()

tvl_data = {"tvl_dux":tvl_dux,"tvl_usd":tvl_usd}
requests.put(url="GOOGLE_FIREBASE_URL_HERE",
           data=json.dumps(tvl_data))

# Volume
volume_daily = final_df.query("name != 'Staked'").groupby("date").sum().sort_index(ascending=False)["values"]

# Holders
holders_from = final_df.query("name != 'Staked'")[["from","values"]].groupby(by="from").sum().reset_index().rename(columns={"from":"holder_address"})
holders_from["values"] = holders_from["values"] * -1

holders_to = final_df.query("name != 'Staked'")[["to","values"]].groupby(by="to").sum().reset_index().rename(columns={"to":"holder_address"})

holders_df = pd.concat([holders_to,holders_from],ignore_index=True).groupby("holder_address").sum().sort_values("values",ascending=False).reset_index()
holders_df = holders_df.query("values > 0").reset_index(drop=True)
# Fix last holder problem
holders_df = holders_df.iloc[:holders_df.shape[0] - 1]
holders_df["%"] = (holders_df["values"] / holders_df["values"].sum())

requests.put(url="GOOGLE_FIREBASE_URL_HERE",
             data=json.dumps(holders_df.to_dict("records")))