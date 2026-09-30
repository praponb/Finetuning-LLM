from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .groupBy(F.year("paid_at").alias("paid_year"), "method", "status")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.min("paid_at").alias("min_paid_at"),
    )
    .filter(F.col("avg_amount") >= 35)
)
