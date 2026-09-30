from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
    .orderBy(F.col("stddev_sqft").desc())
)
