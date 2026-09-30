from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNull())
    .select("id", "product_name", "category", F.explode("tags").alias("label"))
)
