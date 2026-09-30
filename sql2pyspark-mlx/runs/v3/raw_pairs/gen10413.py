from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.shipping_fee") > 39)
    .select(F.col("oi.id"), F.col("oi.quantity"), F.col("o.amount"), F.col("o.channel"))
    .orderBy(F.col("amount").desc(), "id")
    .limit(20)
)
