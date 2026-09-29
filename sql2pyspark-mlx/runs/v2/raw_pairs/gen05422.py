from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .filter(F.col("unit_price") > 110)
    .groupBy("order_id")
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("oibo"), F.col("o.id") == F.col("oibo.order_id"), "inner")
    .select(F.col("o.id"), F.col("o.customer_id"), F.col("oibo.unit_price_range"))
)
