from pyspark.sql import functions as F

recent_customers = df_customers.filter(F.col("country") != "US")

agg = (
    recent_customers
    .groupBy("country", F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
)

result = (
    agg
    .select("country", "signup_month", "avg_age", "stddev_credit_limit")
    .orderBy("country", F.col("stddev_credit_limit").desc())
)
