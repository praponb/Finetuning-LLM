from pyspark.sql import functions as F

order_stats = (
    df_payments
    .filter(F.col("paid_at") < "2024-09-01")
    .groupBy("order_id")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.status"),
        F.col("os.avg_amount"),
    )
)
