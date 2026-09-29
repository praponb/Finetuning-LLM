from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.alias("order_items"), F.col("orders.id") == F.col("order_items.order_id"), "left_anti")
    .filter(F.col("orders.shipping_fee").between(16, 17))
    .select(F.col("orders.id"), F.col("orders.store_id"), F.col("orders.order_date"))
)
