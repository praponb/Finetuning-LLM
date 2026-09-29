from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left")
    .groupBy(F.col("o.channel"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.min(F.col("oi.unit_price")).alias("min_unit_price"),
    )
    .filter(F.col("order_item_count") >= 5)
    .orderBy("min_unit_price", "channel")
    .limit(5)
)
