from pyspark.sql import functions as F

src = (
    df_stores
    .filter(F.col("region") != "North")
    .withColumn("city_prefix", F.substring("city", 1, 2))
)

summary = (
    src
    .groupBy("city")
    .agg(F.avg("sqft").alias("avg_sqft"))
)

result = (
    summary
    .select("city", "avg_sqft")
    .orderBy(F.col("avg_sqft").desc())
)
