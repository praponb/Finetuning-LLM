from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(460, 1030))
    .select("id", "price", "category", F.explode("tags").alias("tag"))
)
