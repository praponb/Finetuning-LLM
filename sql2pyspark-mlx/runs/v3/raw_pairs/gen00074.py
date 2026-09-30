from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(F.min("paid_at").alias("min_paid_at"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.shipping_fee"),
        F.col("t.min_paid_at"),
    )
    .orderBy(F.col("min_paid_at").desc(), "id")
    .limit(10)
)
