from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNull())
    .select("cost", F.size("tags").alias("tags_count"))
)
