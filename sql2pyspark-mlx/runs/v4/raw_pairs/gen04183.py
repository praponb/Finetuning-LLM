from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("order_id").isin([15, 20, 31, 38]))
    .groupBy("order_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("os.total_unit_price"),
        F.col("os.avg_quantity"),
    )
)
