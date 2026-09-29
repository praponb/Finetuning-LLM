from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"), "city")
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
    .filter(F.col("stddev_sqft") >= 78)
    .orderBy(F.col("city").desc(), "opened_month")
    .limit(5)
)
