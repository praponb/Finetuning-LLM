from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.filter(F.col("order_id") == 27).alias("order_items"), F.col("orders.id") == F.col("order_items.order_id"), "left_anti")
    .select(
        F.col("orders.id"),
        F.col("orders.channel"),
        F.col("orders.store_id"),
        F.col("orders.shipping_fee"),
    )
    .orderBy(F.col("store_id").desc())
)
