from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") > 630)
    .select("id", "brand", F.explode("tags").alias("tag_name"))
)
