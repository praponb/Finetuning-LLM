from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 32)
    .select("id", "price", F.explode("tags").alias("tag_name"))
)
