from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .filter(F.col("unit_price").between(210, 320))
    .groupBy("order_id")
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.unit_price_range") >= 21)
    .select(F.col("o.channel"), F.col("t.unit_price_range"))
)
