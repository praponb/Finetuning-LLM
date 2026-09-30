from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.count("balance").alias("balance_count"))
    .filter(F.col("balance_count") < 8)
    .select("opened_quarter", "balance_count")
)
