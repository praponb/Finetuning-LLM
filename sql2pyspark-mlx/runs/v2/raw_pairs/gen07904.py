from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") <= 870)
    .select("id", "category", "price", F.explode("tags").alias("tag_name"))
)
