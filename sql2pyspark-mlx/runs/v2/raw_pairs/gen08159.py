from pyspark.sql import functions as F

order_stats = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .groupBy("order_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.max("paid_at").alias("last_paid_at"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.avg_amount") >= 1090)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("os.avg_amount"),
        F.col("os.last_paid_at"),
    )
)
