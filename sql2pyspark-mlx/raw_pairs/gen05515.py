from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") <= 1000)
    .select("id", "brand", F.explode("tags").alias("tag_name"))
)
