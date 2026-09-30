from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Initech")
    .select("id", "product_name", "price", F.explode("tags").alias("tag"))
)
