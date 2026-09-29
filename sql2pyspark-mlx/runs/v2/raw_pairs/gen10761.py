from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "bestseller"))
    .select("id", "category", "price", F.explode("tags").alias("tag"))
)
