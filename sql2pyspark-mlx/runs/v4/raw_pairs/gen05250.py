from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter(F.col("order_id").isNotNull() & ~F.col("status").isin(["SUCCESS", "FAILED"]))
    .withColumn("amount_rounded", F.round("amount", 0))
    .withColumn("paid_at_dayofweek", F.dayofweek("paid_at"))
)

result = (
    cleaned
    .groupBy("status", "order_id")
    .pivot("method", ["paypal", "bank_transfer", "card"])
    .agg(F.avg("paid_at_dayofweek").alias("value_sum"))
)
