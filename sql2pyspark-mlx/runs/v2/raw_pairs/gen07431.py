from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.count("*").alias("cnt"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.revenue") >= 56)
    .select(F.col("o.id"), F.col("o.customer_id"), F.col("os.cnt"), F.col("os.revenue"))
)
