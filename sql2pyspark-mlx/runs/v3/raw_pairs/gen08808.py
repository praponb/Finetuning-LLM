from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") > 1050)
    .select("id", "price", "product_name", F.explode("tags").alias("tag"))
)
