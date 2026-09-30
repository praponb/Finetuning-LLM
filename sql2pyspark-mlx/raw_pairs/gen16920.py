from pyspark.sql import functions as F

recent_transactions = df_transactions.filter(F.col("txn_type") == "REFUND")

summary = (
    recent_transactions
    .groupBy("is_flagged", F.to_date("txn_time").alias("txn_day"))
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
    )
)

result = (
    summary
    .filter(F.col("stddev_amount") < 24)
    .orderBy(F.col("refund_count").desc())
)
