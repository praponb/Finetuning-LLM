from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.count("age").alias("age_count"),
        F.min("credit_limit").alias("min_credit_limit"),
    )
)
