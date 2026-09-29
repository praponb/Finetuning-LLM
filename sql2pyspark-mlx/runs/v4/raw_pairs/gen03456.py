from pyspark.sql import functions as F

result = (
    df_products
    .select(F.explode("tags").alias("label"))
    .groupBy("label")
    .agg(F.count("*").alias("product_count"))
    .orderBy(F.col("product_count").desc(), "label")
)
