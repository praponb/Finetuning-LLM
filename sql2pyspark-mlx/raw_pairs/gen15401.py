from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") >= 620)
    .select("id", "category", F.explode("tags").alias("tag_name"))
)
