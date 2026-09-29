from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.filter(F.col("product_id") == 8).alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_anti")
    .select(F.col("o.id"), F.col("o.store_id"))
)
