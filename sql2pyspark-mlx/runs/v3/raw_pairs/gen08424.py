from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("order_stats"), F.col("orders.id") == F.col("order_stats.order_id"), "inner")
    .filter(F.col("order_stats.avg_quantity") > 2.8)
    .select(
        F.col("orders.channel"),
        F.col("orders.customer_id"),
        F.col("order_stats.min_quantity"),
        F.col("order_stats.avg_quantity"),
    )
    .orderBy("channel")
)
