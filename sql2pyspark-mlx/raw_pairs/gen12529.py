from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "FROZEN")
    .groupBy("account_type")
    .agg(
        F.count("*").alias("cnt"),
        F.max("opened_date").alias("last_opened_date"),
    )
)
