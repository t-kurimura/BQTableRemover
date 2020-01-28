# DWH層のスキーマに変更があったときに、過去分のデータをバルクで一気に実行する
from google.cloud import bigquery
from google.oauth2 import service_account
from google_auth_oauthlib import flow

import csv
import datetime


appflow = flow.InstalledAppFlow.from_client_secrets_file(
    "./credentials.json", scopes=["https://www.googleapis.com/auth/bigquery"]
)
appflow.run_console()
credentials = appflow.credentials

project_id = "project_id"
dataset_id = "project_id"

client = bigquery.Client(project=project_id, credentials=credentials)


def date_range_generator(starts_date, ends_date, step):
    start = date_str_to_int(starts_date)
    end = date_str_to_int(ends_date)

    dt = datetime.date(*start)
    end_dt = datetime.date(*end)
    while dt <= end_dt:
        yield dt
        dt += datetime.timedelta(days=step)


def date_str_to_int(date_str):
    if "-" in date_str and "/" not in date_str:
        ymd = tuple([int(exp) for exp in date_str.split("-")])
    elif "/" in date_str and "-" not in date_str:
        ymd = tuple([int(exp) for exp in date_str.split("/")])
    else:
        ymd = "a"

    return ymd


def date_hour_range_generator(starts_datetime, ends_datetime, step_hour=1):
    step = datetime.timedelta(hours=step_hour)
    result = []
    while starts_datetime.timestamp() <= ends_datetime.timestamp():
        result.append(starts_datetime.strftime("%Y%m%d%H"))
        starts_datetime += step
    return result


def __rm_daily(table_suffix, start_date_str, end_date_str):
    start_datetime = datetime.datetime.strptime(start_date_str, "%Y%m%d")
    start_date = datetime.date(
        start_datetime.year, start_datetime.month, start_datetime.day
    )

    end_datetime = datetime.datetime.strptime(end_date_str, "%Y%m%d")
    end_date = datetime.date(end_datetime.year, end_datetime.month, end_datetime.day)

    for step_start_date in date_range_generator(
        start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), 1
    ):
        table_id = "{}.{}.{}{}".format(
            project_id, dataset_id, table_suffix, step_start_date.strftime("%Y%m%d")
        )
        result = client.delete_table(table_id, not_found_ok=True)
        print("{} : {}".format(table_id, result))


def __rm_hourly(table_suffix, start_date_str, end_date_str):
    start_date = datetime.datetime.strptime(start_date_str, "%Y%m%d%H")
    end_date = datetime.datetime.strptime(end_date_str, "%Y%m%d%H")

    for step_start_date in date_hour_range_generator(start_date, end_date, 1):
        table_id = "{}.{}.{}{}".format(
            project_id, dataset_id, table_suffix, step_start_date
        )
        result = client.delete_table(table_id, not_found_ok=True)
        print("{} : {}".format(table_id, result))


def __rm_tables(table_names):
    for i in range(len(table_names)):
        table_id = "{}.{}.{}".format(project_id, dataset_id, table_names[i])
        result = client.delete_table(table_id, not_found_ok=True)
        print("{} : {}".format(table_id, result))


def __retrieve_list_from_csv(path):
    each_items = []
    with open(path) as f:
        reader = csv.reader(f)
        for row in reader:
            each_items.append(row[0])
    return each_items


__rm_daily("table_prefix", "20190401", "20191007"),
# __rm_daily("table_prefix", "20190808", "20191117"),
# __rm_hourly("table_prefix", "2019080809", "2019080809")
# __rm_hourly("table_prefix", "2019080809", "2019080809")
# __rm_hourly("table_prefix "2019111813", "2019111813")
# __rm_hourly("table_prefix "2019111813", "2019111813")
# __rm_hourly("table_prefix", "2019111401", "2019111401")
# __rm_hourly("table_prefix", "2019111401", "2019111401")
# __rm_hourly("table_prefix", "2017121808", "2017121808")
# __rm_hourly("table_prefix "2019111401", "2019111401")
# __rm_hourly("table_prefix "2019111401", "2019111401")
# __rm_hourly("table_prefix, "2019111810", "2019111810")
# __rm_hourly("table_prefix, "2019111810", "2019111810")
# __rm_hourly("table_prefix_", "2019080809", "2019080809")
# __rm_hourly("table_prefix", "2019111401", "2019111401")
# __rm_hourly("table_prefix", "2019111400", "2019111400")
# CSVに記述しているファイル一覧を一気に消す
# __rm_tables(
#     __retrieve_list_from_csv(
#         "./data/{}.csv"
#     )
# )
