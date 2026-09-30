from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull() & F.col("status").isin(["FAILED", "REFUNDED", "SUCCESS"]))
    .groupBy("method")
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy("avg_amount")
)
