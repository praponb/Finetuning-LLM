from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([10, 19, 34]))
    .select("id", "brand", "category", F.explode("tags").alias("tag"))
)
