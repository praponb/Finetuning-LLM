from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .filter(F.col("products.cost") > 310)
    .select(
        F.col("order_items.order_id"),
        F.col("order_items.product_id"),
        F.col("products.price"),
        F.col("products.launch_date"),
        F.col("orders.amount"),
    )
)
