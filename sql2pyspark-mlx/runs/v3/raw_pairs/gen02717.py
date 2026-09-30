from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.month(F.col("o.order_date")).isin([3, 7, 12]) & F.col("oi.order_id").isin([29, 36, 39]))
    .select(F.col("o.shipping_fee"), F.col("oi.product_id"))
)
