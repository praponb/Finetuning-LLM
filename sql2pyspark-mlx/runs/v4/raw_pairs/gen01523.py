from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(800, 860))
    .select("id", "product_name", "price", F.explode("tags").alias("label"))
)
