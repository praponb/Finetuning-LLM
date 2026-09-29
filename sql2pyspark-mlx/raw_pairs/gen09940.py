from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "inner")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("products.cost") <= 550)
    .select(
        F.col("order_items.id"),
        F.col("order_items.unit_price"),
        F.col("order_items.product_id"),
        F.col("products.launch_date"),
        F.col("orders.shipping_fee"),
    )
)
