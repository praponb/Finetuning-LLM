from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(870, 920))
    .select("id", "category", F.explode("tags").alias("label"))
)
