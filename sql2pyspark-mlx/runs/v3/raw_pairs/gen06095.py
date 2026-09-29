from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") >= "2024-09-03")
    .groupBy("status", "account_type")
    .agg(F.sum("balance").alias("total_balance"))
    .orderBy("status", F.col("total_balance").desc())
)
