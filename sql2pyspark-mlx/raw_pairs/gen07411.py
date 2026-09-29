from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .filter(F.array_contains(F.col("products.tags"), "sale") & (F.col("order_items.order_id") == 16))
    .select(
        F.col("order_items.product_id"),
        F.col("order_items.unit_price"),
        F.col("products.launch_date"),
        F.col("products.category"),
        F.col("orders.order_date"),
        F.col("orders.store_id"),
    )
)
