from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") != "Globex")
    .select("id", "brand", "category", F.explode("tags").alias("tag"))
)
