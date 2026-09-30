from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.min("age").alias("min_age"),
    )
    .filter(F.col("min_age") > 54)
    .orderBy("signup_quarter")
    .limit(5)
)
