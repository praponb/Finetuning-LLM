from pyspark.sql import functions as F

result = (
    df_stores
    .agg(
        F.sum(F.when(F.col("region") == "East", 1).otherwise(0)).alias("east_count"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.min("sqft").alias("min_sqft"),
    )
    .orderBy("stddev_sqft")
)
