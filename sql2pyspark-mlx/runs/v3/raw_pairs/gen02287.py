from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") <= 52000) & (F.col("status") == "FROZEN"))
    .groupBy("account_type")
    .agg(F.min("balance").alias("min_balance"))
    .orderBy("account_type")
)
