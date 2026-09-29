from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.shipping_fee"),
        F.col("os.avg_quantity"),
        F.col("os.unit_price_range"),
    )
    .orderBy("status")
)
