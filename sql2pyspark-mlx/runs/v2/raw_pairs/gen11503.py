from pyspark.sql import functions as F

base = df_customers.filter(F.col("signup_date") >= "2023-08-01")

summary = (
    base
    .groupBy(F.quarter("signup_date").alias("signup_quarter"), "segment")
    .agg(
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.avg("age").alias("avg_age"),
    )
)

result = (
    summary
    .select("signup_quarter", "segment", "stddev_age", "avg_age")
    .orderBy(F.col("signup_quarter").desc(), "segment")
)
