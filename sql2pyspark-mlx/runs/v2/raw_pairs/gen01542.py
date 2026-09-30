from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"), "is_active")
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
    .filter(F.col("avg_credit_limit") >= 9000)
    .select("signup_month", "is_active", "age_range", "avg_credit_limit")
)
