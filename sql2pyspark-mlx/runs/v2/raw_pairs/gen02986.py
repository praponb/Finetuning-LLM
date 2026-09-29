from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .groupBy(F.col("p.id"), F.col("p.category"))
    .agg(F.count(F.col("oi.id")).alias("order_item_count"))
    .filter(F.col("order_item_count") > 1)
    .orderBy(F.col("order_item_count").desc())
)
