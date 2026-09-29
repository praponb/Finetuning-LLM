from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"), "city")
    .agg(
        F.min("signup_date").alias("min_signup_date"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
    .filter(F.col("avg_age") >= 77)
    .orderBy(F.col("signup_quarter").desc(), "min_signup_date", "city")
    .limit(1)
)
