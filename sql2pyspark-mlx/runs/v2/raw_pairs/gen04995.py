from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("order_items.quantity") != 8)
    .select(
        F.col("order_items.id"),
        F.col("order_items.unit_price"),
        F.col("order_items.quantity"),
        F.col("orders.store_id"),
        F.col("orders.shipping_fee"),
        F.col("stores.store_name"),
        F.col("stores.region"),
    )
    .orderBy("store_id")
)
