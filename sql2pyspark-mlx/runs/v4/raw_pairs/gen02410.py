from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "left")
    .filter(F.array_contains(F.col("products.tags"), "eco") & (F.col("order_items.unit_price") < 270))
    .select(
        F.col("order_items.id"),
        F.col("order_items.unit_price"),
        F.col("orders.shipping_fee"),
        F.col("orders.store_id"),
        F.col("products.supplier_id"),
    )
)
