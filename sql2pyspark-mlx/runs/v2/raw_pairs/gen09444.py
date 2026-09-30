from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left")
    .groupBy(F.col("o.id"), F.col("o.status"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.max(F.col("oi.unit_price")).alias("max_unit_price"),
    )
    .orderBy(F.col("order_item_count").desc())
)
