from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.min("paid_at").alias("min_paid_at"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("order_stats"), F.col("orders.id") == F.col("order_stats.order_id"), "inner")
    .select(
        F.col("orders.id"),
        F.col("orders.shipping_fee"),
        F.col("order_stats.amount_count"),
        F.col("order_stats.min_paid_at"),
    )
    .orderBy(F.col("shipping_fee").desc(), "id")
    .limit(10)
)
