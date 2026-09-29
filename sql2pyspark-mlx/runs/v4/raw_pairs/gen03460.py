from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.upper(F.trim("product_name")) == "NOISE-CANCEL HEADPHONES")
    .select("id", "brand", F.explode("tags").alias("tag"))
)
