from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.length("product_name") >= 8)
    .select("id", "price", F.explode("tags").alias("tag_name"))
)
