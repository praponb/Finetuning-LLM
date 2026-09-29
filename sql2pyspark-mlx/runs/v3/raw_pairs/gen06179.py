from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .groupBy("product_id")
    .agg(F.count("*").alias("inventory_count"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(F.col("p.id"), F.col("p.brand"), F.col("t.inventory_count"))
)
