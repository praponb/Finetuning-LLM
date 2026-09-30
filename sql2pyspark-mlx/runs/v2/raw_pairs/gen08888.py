from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") >= 410)
    .select("id", "price", F.explode("tags").alias("tag"))
)
