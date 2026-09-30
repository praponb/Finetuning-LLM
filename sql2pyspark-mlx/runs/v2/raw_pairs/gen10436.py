from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").isNotNull() & (F.col("sqft") > 10540))
    .groupBy("region")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("min_sqft") >= 12900)
    .drop("min_sqft")
    .orderBy(F.col("berlin_count").desc(), F.col("region").desc())
)
