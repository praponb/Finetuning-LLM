from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") >= "2024-08-16")
    .select("region", F.lower("city").alias("city_lower"))
)
