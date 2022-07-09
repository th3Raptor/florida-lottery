import requests
from bs4 import BeautifulSoup
import re
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
import json


def execute_sql(sql: str) -> bool:
    try:
        with open(Path(r"config.json"), "r") as configuration_file:
            config = json.load(configuration_file)

        env = config['environment']
        params = config['database_credentials'][env]

        conn = psycopg2.connect(
            host=params['host'],
            database=params['database_name'],
            user=params['username'],
            password=params['password'],
            cursor_factory=RealDictCursor
        )
        cursor = conn.cursor()

        res = cursor.execute(sql)
        conn.commit()

        cursor.close()
        conn.close()

        return True
    except Exception as ex:
        print(f"{ex}")
        return False


def main():
    page = requests.get(url_powerball)
    soup = BeautifulSoup(page.content, "html.parser")
    tables = soup.find_all(name="table")

    # row_filter = r"((\d{2}/){2}\d{2}\d{1,2}(-\d{1,2}){4}PB\d{1,2}X\d)+"
    data_filter = r"(\d{2}/\d{2}/\d{2})(\d{1,2})-(\d{1,2})-(\d{1,2})-(\d{1,2})-(\d{1,2})PB(\d{1,2})X(\d)"

    data_line = ''
    for table in tables:
        for row in table.find_all(name="tr"):
            if len(x := row.text.strip().replace("\n", '')) > 0 and re.compile(data_filter).match(x):
                data_line += x
        # break
    data = re.findall(data_filter, data_line)
    sql = f""" 
        INSERT INTO sdl.florida_lottery(date_stamp, number1, number2, number3, number4, number5, bonus, multiplier)
        VALUES
            {", ".join(map(str, data))}
    """
    if execute_sql(sql):
        print('Written')


if __name__ == '__main__':
    url_powerball = "https://www.flalottery.com/exptkt/pb.htm"
    url_mega_millions = "https://www.flalottery.com/exptkt/mmil.htm"

    main()
