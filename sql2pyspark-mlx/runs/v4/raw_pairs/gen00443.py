from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") >= 820)
    .select("id", "price", "brand", F.explode("tags").alias("tag_name"))
)
