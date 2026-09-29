from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "SAVINGS") | (F.col("status") != "FROZEN"))
    .groupBy("account_type")
    .agg(F.count("*").alias("total_count"))
)
