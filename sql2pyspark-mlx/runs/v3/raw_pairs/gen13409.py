from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .groupBy("order_id")
    .agg(F.count("unit_price").alias("unit_price_count"))
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("oibo"), F.col("o.id") == F.col("oibo.order_id"), "left")
    .select(F.col("o.discount"), F.col("o.channel"), F.col("oibo.unit_price_count"))
)
