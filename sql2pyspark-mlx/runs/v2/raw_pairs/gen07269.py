from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.length("store_name") >= 12)
    .select("id", "region", "sqft", F.length("region").alias("region_length"))
)
