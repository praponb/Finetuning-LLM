from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy("country", F.month("signup_date").alias("signup_month"))
    .agg(
        F.max("signup_date").alias("last_signup_date"),
        F.count("credit_limit").alias("credit_limit_count"),
        F.min("signup_date").alias("min_signup_date"),
    )
)
