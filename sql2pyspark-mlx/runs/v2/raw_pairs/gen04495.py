from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Watch%"))
    .select("id", "category", "price", F.explode("tags").alias("tag_name"))
)
