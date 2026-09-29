from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") >= 850)
    .select("id", "brand", F.explode("tags").alias("tag"))
)
