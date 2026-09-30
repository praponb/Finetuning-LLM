from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(F.min("last_restock").alias("min_last_restock"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(F.col("p.brand"), F.col("p.launch_date"), F.col("ps.min_last_restock"))
    .orderBy("brand")
)
