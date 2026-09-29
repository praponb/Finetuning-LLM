from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("products.product_name").like("De%"))
    .select(
        F.col("order_items.order_id"),
        F.col("order_items.quantity"),
        F.col("products.supplier_id"),
        F.col("orders.store_id"),
        F.col("orders.amount"),
    )
)
