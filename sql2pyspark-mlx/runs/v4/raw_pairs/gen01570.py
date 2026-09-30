from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.length(F.col("p.product_name")) < 10)
    .groupBy(F.col("p.id"), F.col("p.product_name"))
    .agg(F.count(F.col("oi.id")).alias("order_item_count"))
    .orderBy(F.col("product_name").desc())
)
