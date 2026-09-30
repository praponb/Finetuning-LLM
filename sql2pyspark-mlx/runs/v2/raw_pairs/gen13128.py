from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(F.count("balance").alias("balance_count"))
    .filter(F.col("balance_count") > 5)
    .select("status", "balance_count")
)
