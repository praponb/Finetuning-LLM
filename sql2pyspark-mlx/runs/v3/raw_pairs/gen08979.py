from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .select(
        F.col("order_items.id"),
        F.col("order_items.unit_price"),
        F.col("orders.customer_id"),
        F.col("stores.store_name"),
        F.col("stores.opened_date"),
    )
)
