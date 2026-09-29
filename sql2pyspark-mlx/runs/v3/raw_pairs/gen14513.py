from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "eco"))
    .select(F.explode("tags").alias("tag_name"))
    .groupBy("tag_name")
    .agg(F.count("*").alias("product_count"))
    .orderBy(F.col("product_count").desc(), "tag_name")
)
