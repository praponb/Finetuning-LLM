from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
    .orderBy("stddev_sqft", F.col("city").desc())
    .limit(3)
)
