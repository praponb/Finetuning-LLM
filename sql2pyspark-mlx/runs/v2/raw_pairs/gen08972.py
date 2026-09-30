from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.sum("sqft").alias("total_sqft"),
    )
)
