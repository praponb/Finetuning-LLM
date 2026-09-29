from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") >= 520)
    .select("id", "brand", F.explode("tags").alias("label"))
)
