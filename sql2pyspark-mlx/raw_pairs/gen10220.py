from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("order_stats"), F.col("orders.id") == F.col("order_stats.order_id"), "inner")
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("orders.store_id"),
        F.col("order_stats.quantity_range"),
    )
    .orderBy("quantity_range")
)
