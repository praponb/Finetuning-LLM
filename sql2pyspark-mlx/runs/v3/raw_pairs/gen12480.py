from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Sports", "Fashion"]))
    .select("id", "brand", "product_name", F.explode("tags").alias("tag"))
)
