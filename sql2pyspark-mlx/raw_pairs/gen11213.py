from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "US")
    .groupBy("is_active", F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.min("signup_date").alias("first_signup_date"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
    .filter(F.col("stddev_credit_limit") < 46)
)
