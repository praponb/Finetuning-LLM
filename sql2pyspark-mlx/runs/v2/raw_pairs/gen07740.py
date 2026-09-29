from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.lower("store_name").like("%iam%") & (F.col("opened_date") >= "2024-06-17") & (F.col("city") != "Tokyo"))
    .select("id", "store_name", F.lower("region").alias("region_lower"))
)
