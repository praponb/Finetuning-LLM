from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "outer")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "outer")
    .filter(F.col("order_items.unit_price").between(200, 290) & F.col("order_items.order_id").isin([14, 32, 34]))
    .select(
        F.col("order_items.id"),
        F.col("order_items.order_id"),
        F.col("products.brand"),
        F.col("products.supplier_id"),
        F.col("orders.store_id"),
    )
)
