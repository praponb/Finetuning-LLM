from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Grocery")
    .select("id", "brand", "price", F.explode("tags").alias("tag_name"))
)
