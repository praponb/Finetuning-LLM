from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") < 220) & ~F.col("product_name").like("%Can%") & ~F.col("category").isin(["Electronics", "Fashion"]))
    .groupBy("category")
    .agg(F.count("*").alias("total_count"))
    .orderBy("category")
)
