from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.filter(F.col("product_id") == 39).alias("order_items"), F.col("orders.id") == F.col("order_items.order_id"), "left_anti")
    .select(F.col("orders.id"), F.col("orders.store_id"))
)
