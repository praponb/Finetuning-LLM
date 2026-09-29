from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(F.col("p.id"), F.col("p.cost"), F.col("ps.max_quantity"))
    .orderBy("id")
)
