from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([27, 28]))
    .select("id", "category", "price", F.explode("tags").alias("label"))
)
