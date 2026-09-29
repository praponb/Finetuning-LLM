from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("os.avg_unit_price"),
        F.col("os.revenue"),
    )
    .orderBy(F.col("avg_unit_price").desc(), F.col("revenue").desc())
)
