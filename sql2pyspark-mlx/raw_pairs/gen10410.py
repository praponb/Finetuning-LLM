from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "premium"))
    .select("id", "product_name", "category", F.explode("tags").alias("tag"))
)
