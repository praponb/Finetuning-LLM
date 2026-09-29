from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter((F.col("p.launch_date") < "2025-10-01") & (F.col("o.order_date") >= "2024-12-08"))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("p.cost"),
        F.col("p.price"),
        F.col("o.discount"),
        F.col("o.store_id"),
        F.trim(F.col("o.channel")).alias("clean_channel"),
    )
)
