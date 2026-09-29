from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNull())
    .select(F.explode("tags").alias("tag_name"))
    .groupBy("tag_name")
    .agg(F.count("*").alias("product_count"))
)
