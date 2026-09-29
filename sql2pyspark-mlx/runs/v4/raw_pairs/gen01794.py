from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([6, 13]))
    .select("id", "price", "brand", F.explode("tags").alias("tag"))
)
