from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(F.countDistinct("quantity").alias("unique_stores"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.unique_stores") >= 8)
    .select(F.col("p.id"), F.col("p.product_name"), F.col("ps.unique_stores"))
    .orderBy("unique_stores", "id")
    .limit(10)
)
