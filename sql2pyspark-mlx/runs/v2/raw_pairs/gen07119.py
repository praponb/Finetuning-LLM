from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "left")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "right")
    .select(
        F.col("order_items.order_id"),
        F.col("order_items.product_id"),
        F.col("products.stock_qty"),
        F.col("products.launch_date"),
        F.col("orders.amount"),
        F.col("orders.store_id"),
    )
    .orderBy(F.col("product_id").desc())
)
