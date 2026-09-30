from pyspark.sql import functions as F

result = (
    df_products
    .select("category", "brand")
    .distinct()
    .orderBy("brand", F.col("category").desc())
)
