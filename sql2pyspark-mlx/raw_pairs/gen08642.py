from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([1, 19, 36]))
    .select(F.explode("tags").alias("tag_name"))
    .groupBy("tag_name")
    .agg(F.count("*").alias("product_count"))
    .orderBy(F.col("product_count").desc(), "tag_name")
)
