import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
import requests

load_dotenv()

response = requests.get("https://api.phase-tracker.com/api/groups")
mappings = response.json()

name_to_phase = {}
for phase, names in mappings.items():
    for name in names:
        name_to_phase[name] = phase.replace(" ", "_")

conn = psycopg2.connect(
    host=os.environ.get('HOST'),
    database=os.environ.get('DB'),
    user="",
    password=os.environ.get('PW')
)

cursor = conn.cursor()
cursor.execute("SELECT DISTINCT name FROM subscriber_data_historical;")
names = cursor.fetchall()
for name_tuple in names:
    name = name_tuple[0]
    sanitized_name = name.replace(" ", "_")
    phase_folder = name_to_phase.get(name, "Uncategorized")
    os.makedirs(phase_folder, exist_ok=True)
    query = """
    SELECT *
    FROM subscriber_data_historical
    WHERE name = %s;
    """
    df = pd.read_sql_query(query, conn, params=(name,))
    output_file = os.path.join(phase_folder, f"{sanitized_name}.csv")
    df.to_csv(output_file, index=False)
    print(f"CSV created: {output_file}")
cursor.close()
conn.close()
