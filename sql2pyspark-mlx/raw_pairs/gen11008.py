from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(((F.col("balance") <= 64000) & (F.col("status") != "FROZEN")) | (F.col("status") != "CLOSED"))
    .groupBy("account_type")
    .agg(F.count("balance").alias("balance_count"))
)
