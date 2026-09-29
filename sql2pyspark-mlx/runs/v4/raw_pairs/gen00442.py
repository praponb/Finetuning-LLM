from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .groupBy("order_id")
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("t.unit_price_range"),
        F.col("t.avg_quantity"),
    )
    .orderBy("unit_price_range")
)
