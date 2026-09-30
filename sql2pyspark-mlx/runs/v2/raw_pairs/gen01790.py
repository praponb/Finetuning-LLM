from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") >= 360)
    .select("id", "price", F.explode("tags").alias("label"))
)
