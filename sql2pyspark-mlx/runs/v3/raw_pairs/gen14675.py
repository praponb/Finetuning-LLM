from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "outer")
    .filter((F.col("orders.store_id") == 40) | F.col("orders.channel").isin(["mobile", "web"]))
    .select(
        F.col("order_items.product_id"),
        F.col("order_items.unit_price"),
        F.col("orders.shipping_fee"),
        F.col("orders.channel"),
    )
    .orderBy("shipping_fee")
)
