from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.size("tags") > 2)
    .select("id", "product_name", "price", F.explode("tags").alias("tag"))
)
