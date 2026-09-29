from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .groupBy("product_id")
    .agg(F.max("last_restock").alias("last_last_restock"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(F.col("p.product_name"), F.col("p.category"), F.col("ps.last_last_restock"))
    .orderBy("last_last_restock")
)
