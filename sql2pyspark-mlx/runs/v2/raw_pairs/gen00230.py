from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") != "South")
    .select("id", "store_name", "city", F.trim("city").alias("clean_city"))
    .orderBy("store_name")
)
