from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .select(
        F.col("oi.product_id"),
        F.col("oi.order_id"),
        F.col("o.channel"),
        F.col("p.product_name"),
        F.lower(F.col("o.channel")).alias("channel_lower"),
    )
    .orderBy(F.col("product_name").desc())
)
