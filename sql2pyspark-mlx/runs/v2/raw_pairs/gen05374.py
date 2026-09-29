from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.min("unit_price").alias("min_unit_price"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.customer_id"),
        F.col("o.status"),
        F.col("os.revenue"),
        F.col("os.min_unit_price"),
    )
    .orderBy(F.col("revenue").desc())
)
