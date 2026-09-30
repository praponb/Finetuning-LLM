from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "West")
    .select("sqft", "city", "region", F.length("store_name").alias("store_name_length"))
    .orderBy(F.col("store_name_length").desc())
)
