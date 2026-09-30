from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter((F.col("o.shipping_fee") <= 33) & (F.col("o.amount") <= 1160))
    .select(F.col("o.status"), F.col("oi.product_id"))
    .orderBy(F.col("product_id").desc())
)
