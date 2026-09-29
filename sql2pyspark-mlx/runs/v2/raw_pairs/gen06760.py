from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") >= 610)
    .select("id", "price", "category", F.explode("tags").alias("tag_name"))
)
