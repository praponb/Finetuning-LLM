from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([11, 19, 36, 40]))
    .select("id", "price", F.explode("tags").alias("tag"))
)
