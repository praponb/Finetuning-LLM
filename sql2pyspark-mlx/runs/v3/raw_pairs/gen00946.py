from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
    .filter(F.col("avg_balance") >= 80)
    .select("customer_id", "avg_balance")
)
