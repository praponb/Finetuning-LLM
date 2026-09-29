from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") != "CLOSED") & (F.col("opened_date").between("2024-06-23", "2024-11-01") | F.col("status").isin(["FROZEN", "OPEN", "CLOSED"])))
)
