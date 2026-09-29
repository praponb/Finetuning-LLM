from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("N%"))
    .select("id", "category", "price", F.explode("tags").alias("tag"))
)
