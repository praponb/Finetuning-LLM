from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .select(
        F.col("order_items.order_id"),
        F.col("orders.channel"),
        F.col("orders.shipping_fee"),
        F.col("products.cost"),
        F.col("products.brand"),
    )
    .orderBy(F.col("cost").desc())
)
