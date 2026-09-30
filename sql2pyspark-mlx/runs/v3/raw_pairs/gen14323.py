from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").isNull())
    .select("id", "product_name", F.explode("tags").alias("tag"))
)
