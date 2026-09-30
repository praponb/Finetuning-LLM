from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.count("age").alias("age_count"),
        F.sum("credit_limit").alias("total_credit_limit"),
        F.min("signup_date").alias("min_signup_date"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
    .filter(F.col("max_credit_limit") < 5200)
    .drop("max_credit_limit")
    .orderBy("age_count", F.col("signup_month").desc())
)
