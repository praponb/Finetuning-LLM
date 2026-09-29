from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter((F.col("opened_date") <= "2023-12-01") | (F.col("opened_date") <= "2024-10-13"))
    .withColumn("city_lower", F.lower("city"))
)

agg = (
    valid_stores
    .groupBy("city")
    .agg(F.sum(F.when(F.col("region") == "North", 1).otherwise(0)).alias("north_count"))
)

result = (
    agg
    .filter(F.col("north_count") < 72)
    .select("city", "north_count")
)
