from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").between(260, 520))
    .select(F.explode("tags").alias("tag"))
    .groupBy("tag")
    .agg(F.count("*").alias("product_count"))
)
