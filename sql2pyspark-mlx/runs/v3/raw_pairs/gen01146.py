from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "inner")
    .filter(F.col("order_items.product_id").isNull())
    .select(
        F.col("order_items.product_id"),
        F.col("order_items.unit_price"),
        F.col("orders.status"),
        F.col("orders.store_id"),
        F.col("products.stock_qty"),
    )
    .orderBy("unit_price")
)
