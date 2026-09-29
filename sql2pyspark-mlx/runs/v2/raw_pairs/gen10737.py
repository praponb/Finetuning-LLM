from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id", "account_type")
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
    .filter(F.col("avg_balance") < 75)
    .select("customer_id", "account_type", "avg_balance")
)
