from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"), "city")
    .agg(
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
        F.max("signup_date").alias("last_signup_date"),
    )
    .filter(F.col("credit_limit_range") > 48)
    .select("signup_month", "city", "credit_limit_range", "last_signup_date")
)
