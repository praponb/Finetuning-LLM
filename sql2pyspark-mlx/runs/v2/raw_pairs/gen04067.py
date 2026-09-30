from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") >= "2024-01-10")
    .select("region", "city")
    .distinct()
    .orderBy("region", "city")
    .limit(10)
)
