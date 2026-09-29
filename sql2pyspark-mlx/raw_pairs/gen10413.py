from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.size("tags") >= 1)
    .select("id", "price", F.explode("tags").alias("label"))
)
