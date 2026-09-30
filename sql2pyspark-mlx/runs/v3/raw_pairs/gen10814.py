from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("oi.order_id").isin([24, 28]) & F.col("oi.product_id").isin([17, 27]))
    .select(F.col("o.amount"), F.col("oi.order_id"))
)
