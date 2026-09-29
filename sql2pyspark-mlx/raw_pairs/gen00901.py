from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "inner")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .filter(F.col("order_items.order_id").isin([15, 18, 24]))
    .select(
        F.col("order_items.id"),
        F.col("order_items.order_id"),
        F.col("order_items.product_id"),
        F.col("products.launch_date"),
        F.col("products.product_name"),
        F.col("orders.status"),
    )
)
