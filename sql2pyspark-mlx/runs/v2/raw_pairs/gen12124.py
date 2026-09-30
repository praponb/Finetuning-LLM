from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") < "2025-10-01")
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)
