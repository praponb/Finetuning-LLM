from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") <= 680)
    .select("id", "brand", "category", F.explode("tags").alias("tag_name"))
)
