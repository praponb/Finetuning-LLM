from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([3, 10, 33]))
    .select("id", "product_name", F.explode("tags").alias("label"))
)
