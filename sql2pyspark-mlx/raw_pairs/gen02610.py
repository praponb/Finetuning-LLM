from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("o.shipping_fee").between(15, 38) & (F.col("oi.quantity") < 4))
    .select(F.col("oi.order_id"), F.col("oi.unit_price"), F.col("o.discount"))
)
