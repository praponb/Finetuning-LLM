from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .filter(F.col("o.order_date") > "2024-07-01")
    .groupBy(F.col("o.status"))
    .agg(F.count(F.col("oi.unit_price")).alias("unit_price_count"))
    .orderBy("status", F.col("unit_price_count").desc())
)
