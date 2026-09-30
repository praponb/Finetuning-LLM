from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([29, 31, 38]))
    .select("id", "price", "product_name", F.explode("tags").alias("tag"))
)
