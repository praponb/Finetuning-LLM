from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("shipping_fee") <= 36).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_semi")
    .select(F.col("oi.id"), F.col("oi.quantity"), F.col("oi.order_id"))
    .orderBy(F.col("order_id").desc(), "id")
    .limit(3)
)
