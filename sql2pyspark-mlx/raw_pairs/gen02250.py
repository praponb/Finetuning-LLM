from pyspark.sql import functions as F

filtered_stores = df_stores.filter((F.col("region") == "East") & (F.col("city") == "Singapore"))

city_summary = (
    filtered_stores
    .groupBy("city")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
)

result = (
    city_summary
    .orderBy(F.col("stddev_sqft").desc(), "city")
    .limit(5)
)
