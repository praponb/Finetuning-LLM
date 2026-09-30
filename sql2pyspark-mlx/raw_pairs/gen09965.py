from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").between(310, 520))
    .select("id", "product_name", F.explode("tags").alias("tag_name"))
)
