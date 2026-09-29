from pyspark.sql import functions as F

result = (
    df_stores
    .select("region", "sqft", "city", F.length("region").alias("region_length"))
)
