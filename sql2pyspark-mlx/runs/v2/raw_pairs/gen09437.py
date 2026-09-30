from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([6, 30, 31]))
    .select("id", "brand", F.explode("tags").alias("tag_name"))
)
