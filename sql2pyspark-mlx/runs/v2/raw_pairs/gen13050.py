from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(400, 500))
    .select("id", "price", F.explode("tags").alias("tag"))
)
