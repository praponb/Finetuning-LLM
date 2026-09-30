from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") <= 330)
    .select("id", "product_name", F.explode("tags").alias("tag"))
)
