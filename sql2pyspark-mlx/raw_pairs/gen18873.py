from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .filter(F.array_contains(F.col("products.tags"), "clearance"))
    .groupBy(F.col("orders.customer_id"))
    .agg(
        F.countDistinct(F.col("order_items.quantity")).alias("unique_orders"),
        F.count(F.col("order_items.unit_price")).alias("unit_price_count"),
    )
    .orderBy(F.col("customer_id").desc(), "unique_orders")
)
