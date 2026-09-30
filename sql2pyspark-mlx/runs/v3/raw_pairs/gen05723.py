from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.alias("order_items"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .groupBy(F.col("orders.id"), F.col("orders.status"))
    .agg(F.count(F.col("order_items.id")).alias("order_item_count"))
    .filter(F.col("order_item_count") >= 5)
)
