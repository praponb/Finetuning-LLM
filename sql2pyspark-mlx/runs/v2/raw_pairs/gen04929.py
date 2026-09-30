from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("oi.quantity") <= 7)
    .groupBy(F.col("o.id"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"),
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
    )
    .orderBy(F.col("order_item_count").desc())
)
