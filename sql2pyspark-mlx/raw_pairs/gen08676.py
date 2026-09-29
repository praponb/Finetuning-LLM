from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Air%"))
    .select("city", F.concat_ws("-", "city", "region").alias("city_region_key"))
)
