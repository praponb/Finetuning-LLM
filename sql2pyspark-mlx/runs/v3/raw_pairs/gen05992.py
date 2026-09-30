from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "product_name", "price", "tags", F.size("tags").alias("tags_count"))
)
