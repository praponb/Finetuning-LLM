from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "price", "brand", F.trim("brand").alias("clean_brand"))
    .orderBy("clean_brand")
)
