from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") > "2024-12-17")
    .select("id", "region", F.lower("region").alias("region_lower"))
    .orderBy("region")
)
