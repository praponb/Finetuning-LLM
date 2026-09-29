from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("order_stats"), F.col("orders.id") == F.col("order_stats.order_id"), "inner")
    .select(
        F.col("orders.customer_id"),
        F.col("orders.shipping_fee"),
        F.col("order_stats.total_quantity"),
    )
    .orderBy(F.col("total_quantity").desc())
)
