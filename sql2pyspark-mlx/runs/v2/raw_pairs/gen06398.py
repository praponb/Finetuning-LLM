from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") != "CLOSED")
    .groupBy("account_type", "status")
    .agg(F.count("balance").alias("balance_count"))
    .filter(F.col("balance_count") < 9)
    .select("account_type", "status", "balance_count")
    .orderBy(F.col("balance_count").desc())
)
