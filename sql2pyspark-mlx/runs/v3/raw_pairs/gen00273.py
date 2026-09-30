from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([13, 21]))
    .select("id", "price", "category", F.explode("tags").alias("tag"))
)
