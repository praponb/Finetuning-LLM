from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.max("signup_date").alias("last_signup_date"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
    .orderBy(F.col("signup_month").desc())
)
