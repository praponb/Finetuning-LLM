from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("quantity").between(4, 6))
    .groupBy("order_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("order_stats"), F.col("orders.id") == F.col("order_stats.order_id"), "inner")
    .filter(F.col("order_stats.cnt") >= 8)
    .select(
        F.col("orders.store_id"),
        F.col("orders.shipping_fee"),
        F.col("order_stats.cnt"),
    )
    .orderBy("shipping_fee", "cnt")
)
