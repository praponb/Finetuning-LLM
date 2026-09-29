from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("order_id").isin([2, 25, 33]))
    .groupBy("order_id")
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.count("unit_price").alias("unit_price_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.status"),
        F.col("os.revenue"),
        F.col("os.unit_price_count"),
    )
)
