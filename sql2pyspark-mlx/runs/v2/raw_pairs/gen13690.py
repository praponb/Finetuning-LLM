from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.count("*").alias("total_count"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .filter(F.col("O.total_count") >= 3)
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("O.total_count"),
        F.col("O.unit_price_range"),
    )
    .orderBy(F.col("unit_price_range").desc(), F.col("total_count").desc())
)
