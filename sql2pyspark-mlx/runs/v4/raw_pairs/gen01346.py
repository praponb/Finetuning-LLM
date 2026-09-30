from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "inner")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .groupBy(F.col("products.brand"))
    .agg(
        F.count(F.col("order_items.quantity")).alias("quantity_count"),
        F.count(F.col("order_items.unit_price")).alias("unit_price_count"),
    )
)
