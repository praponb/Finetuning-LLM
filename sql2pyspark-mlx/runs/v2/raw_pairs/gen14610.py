from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Initech")
    .select("id", "brand", "price", F.explode("tags").alias("label"))
)
