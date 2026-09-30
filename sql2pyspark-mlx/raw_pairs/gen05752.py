from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("OrderStats"), F.col("orders.id") == F.col("OrderStats.order_id"), "inner")
    .filter(F.col("OrderStats.avg_unit_price") > 180)
    .select(
        F.col("orders.id"),
        F.col("orders.channel"),
        F.col("OrderStats.avg_unit_price"),
        F.col("OrderStats.unit_price_range"),
    )
    .orderBy("unit_price_range", "id")
    .limit(1)
)
