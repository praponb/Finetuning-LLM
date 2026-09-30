from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") < 480)
    .select("id", "category", "brand", F.explode("tags").alias("tag"))
)
