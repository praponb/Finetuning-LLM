from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") < 430)
    .select("id", "product_name", F.explode("tags").alias("label"))
)
