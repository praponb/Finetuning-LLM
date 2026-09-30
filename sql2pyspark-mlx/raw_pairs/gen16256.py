from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select("id", "brand", "price", F.explode("tags").alias("label"))
)
