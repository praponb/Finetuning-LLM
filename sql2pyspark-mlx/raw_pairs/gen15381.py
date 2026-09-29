from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("order_stats"), F.col("orders.id") == F.col("order_stats.order_id"), "left")
    .filter(F.col("order_stats.avg_quantity") >= 48)
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("order_stats.avg_quantity"),
    )
    .orderBy("id", F.col("avg_quantity").desc())
)
