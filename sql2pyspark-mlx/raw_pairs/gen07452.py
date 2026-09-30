from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([21, 25]))
    .select("id", "product_name", F.explode("tags").alias("tag"))
)
