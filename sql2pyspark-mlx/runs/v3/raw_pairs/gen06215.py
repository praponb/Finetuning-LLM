from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([9, 27, 29, 31]))
    .select("id", "category", "price", F.explode("tags").alias("tag"))
)
