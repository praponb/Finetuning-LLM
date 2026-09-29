from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") > "2023-11-01")
    .select("id", "region", F.lower("city").alias("city_lower"))
)
