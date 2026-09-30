from pyspark.sql import functions as F

result = (
    df_products
    .select(F.explode("tags").alias("tag"))
    .groupBy("tag")
    .agg(F.count("*").alias("product_count"))
)
