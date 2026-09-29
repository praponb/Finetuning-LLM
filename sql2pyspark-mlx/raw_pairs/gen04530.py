from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Riverside%"))
    .groupBy("city")
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
    .orderBy(F.col("city").desc())
    .limit(10)
)
