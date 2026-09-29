from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([15, 16, 25, 26]))
    .select("id", "price", F.explode("tags").alias("tag"))
)
