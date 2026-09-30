from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") >= "2025-08-01") & F.col("credit_limit").isNotNull() & F.col("name").like("%smit%"))
    .groupBy(
        "country",
        F.date_trunc("MONTH", "signup_date").alias("signup_month"),
        "city",
    )
    .agg(F.min("signup_date").alias("first_signup_date"))
    .orderBy("signup_month")
)
