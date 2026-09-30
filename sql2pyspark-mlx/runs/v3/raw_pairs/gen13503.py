from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") > "2025-01-14")
    .groupBy("status", "account_type")
    .agg(F.max("balance").alias("max_balance"))
    .filter(F.col("max_balance") > 30000)
)
