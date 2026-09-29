from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .groupBy(F.col("orders.status"))
    .agg(
        F.count(F.col("order_items.unit_price")).alias("unit_price_count"),
        F.count(F.col("order_items.quantity")).alias("quantity_count"),
    )
)
