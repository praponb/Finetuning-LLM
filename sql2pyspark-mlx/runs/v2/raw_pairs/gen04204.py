from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.lower(F.trim("product_name")) == "laptop pro 14")
    .select("id", "category", F.explode("tags").alias("tag"))
)
