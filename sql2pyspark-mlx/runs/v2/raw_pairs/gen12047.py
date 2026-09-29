from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("p.launch_date"),
        F.col("p.brand"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
    )
    .orderBy("launch_date")
)
