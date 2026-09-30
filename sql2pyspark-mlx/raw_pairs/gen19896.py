from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("last_restock") > "2023-09-01")
    .groupBy("product_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.unique_products") > 6)
    .select(F.col("p.launch_date"), F.col("ps.unique_products"))
)
