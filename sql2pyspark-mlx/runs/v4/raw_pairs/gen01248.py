from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([15, 33]))
    .select("id", "price", "brand", F.explode("tags").alias("tag_name"))
)
