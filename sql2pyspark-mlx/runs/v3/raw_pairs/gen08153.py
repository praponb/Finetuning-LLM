from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select("id", "category", F.explode("tags").alias("tag"))
)
