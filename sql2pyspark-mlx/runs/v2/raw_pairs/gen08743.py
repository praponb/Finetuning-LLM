from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(F.round(F.stddev("age"), 2).alias("stddev_age"))
    .filter(F.col("stddev_age") >= 64)
    .select("is_active", "stddev_age")
    .orderBy(F.col("stddev_age").desc(), "is_active")
    .limit(50)
)
