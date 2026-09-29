from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("o.channel").isNull() & (F.col("oi.unit_price") <= 280))
    .select(F.col("oi.id"), F.col("oi.unit_price"), F.col("o.shipping_fee"))
)
