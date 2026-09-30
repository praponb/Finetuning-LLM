from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("oi.product_id").isNull())
    .select(F.col("o.shipping_fee"), F.col("o.store_id"), F.col("oi.product_id"))
    .orderBy("product_id", "shipping_fee")
)
