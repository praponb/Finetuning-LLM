from pyspark.sql import functions as F

result = (
    df_stores
    .cube("city", "region")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.sum(F.when(F.col("region") == "North", 1).otherwise(0)).alias("north_count"),
        F.max("sqft").alias("max_sqft"),
    )
)
