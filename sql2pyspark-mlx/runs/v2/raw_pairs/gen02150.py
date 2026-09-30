from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") < 670)
    .select(F.explode("tags").alias("tag_name"))
    .groupBy("tag_name")
    .agg(F.count("*").alias("product_count"))
    .orderBy(F.col("product_count").desc(), "tag_name")
)
