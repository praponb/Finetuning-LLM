from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "inner")
    .filter(F.array_contains(F.col("products.tags"), "bestseller"))
    .select(
        F.col("order_items.product_id"),
        F.col("order_items.unit_price"),
        F.col("orders.channel"),
        F.col("orders.customer_id"),
        F.col("products.stock_qty"),
    )
    .orderBy(F.col("stock_qty").desc())
)
