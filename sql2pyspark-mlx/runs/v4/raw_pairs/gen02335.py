from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Chiang Mai") & F.lower("store_name").like("%irp%"))
    .groupBy("city")
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
    .filter(F.col("stddev_sqft") >= 70)
    .orderBy(F.col("city").desc(), "stddev_sqft")
    .limit(3)
)
