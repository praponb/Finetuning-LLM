from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2025-07-01")
    .groupBy(F.month("txn_time").alias("txn_month"), "merchant")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.min("txn_time").alias("min_txn_time"),
    )
    .filter(F.col("avg_amount") > 3700)
)
