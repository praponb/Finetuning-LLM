from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", "city")
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
    .orderBy(F.col("city").desc())
)
