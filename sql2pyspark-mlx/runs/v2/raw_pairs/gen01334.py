from pyspark.sql import functions as F

src = (
    df_stores
    .filter(F.col("opened_date") < "2025-07-23")
    .withColumn("city_length", F.length("city"))
)

stores_stats = (
    src
    .groupBy("city")
    .agg(
        F.round(F.avg("city_length"), 2).alias("avg_city_length"),
        F.count("city_length").alias("city_length_count"),
    )
)

result = (
    stores_stats
    .filter(F.col("avg_city_length") >= 47)
    .select("city", "avg_city_length", "city_length_count")
    .orderBy("city")
    .limit(10)
)
