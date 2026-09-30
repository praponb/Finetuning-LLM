from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") != "Stark")
    .select("id", "price", F.explode("tags").alias("tag"))
)
