from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.avg("sqft").alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") > 7600)
    .drop("avg_sqft")
    .orderBy(F.col("stddev_sqft").desc(), F.col("region").desc())
)
