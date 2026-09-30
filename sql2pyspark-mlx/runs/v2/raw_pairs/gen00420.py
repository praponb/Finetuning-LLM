from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "outer")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "outer")
    .filter(F.col("order_items.product_id").isNull())
    .select(
        F.col("order_items.id"),
        F.col("order_items.order_id"),
        F.col("order_items.unit_price"),
        F.col("products.supplier_id"),
        F.col("products.stock_qty"),
        F.col("orders.customer_id"),
    )
)
