from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("B%"))
    .select("id", "category", F.explode("tags").alias("tag_name"))
)
