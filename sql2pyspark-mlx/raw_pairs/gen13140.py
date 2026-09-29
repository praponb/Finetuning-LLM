from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(F.max("unit_price").alias("max_unit_price"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .select(F.col("o.shipping_fee"), F.col("t.max_unit_price"))
    .orderBy("max_unit_price")
)
