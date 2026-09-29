from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "inner")
    .select(
        F.col("order_items.id"),
        F.col("order_items.quantity"),
        F.col("orders.amount"),
        F.col("orders.channel"),
        F.col("products.price"),
        F.ceil(F.col("order_items.unit_price")).alias("unit_price_whole"),
    )
    .orderBy(F.col("id").desc())
)
