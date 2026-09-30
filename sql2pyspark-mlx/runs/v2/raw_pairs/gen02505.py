from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.category").isin(["Grocery", "Sports"]))
    .groupBy(F.col("p.category"))
    .agg(F.count(F.col("oi.id")).alias("order_item_count"))
)
