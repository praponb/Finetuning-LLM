from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") != "Home")
    .select("id", "price", F.explode("tags").alias("label"))
)
