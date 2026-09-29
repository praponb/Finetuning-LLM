from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "right")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .select(
        F.col("order_items.unit_price"),
        F.col("order_items.quantity"),
        F.col("orders.order_date"),
        F.col("orders.shipping_fee"),
        F.col("products.product_name"),
        F.col("products.launch_date"),
        F.trim(F.col("orders.channel")).alias("clean_channel"),
    )
    .orderBy("order_date")
)
