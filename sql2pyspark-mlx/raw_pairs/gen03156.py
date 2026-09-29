from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter((F.col("segment") != "Corporate") & (F.col("segment") == "Corporate"))
)

country_summary = (
    recent_customers
    .groupBy("country")
    .agg(
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
        F.min("signup_date").alias("first_signup_date"),
    )
)

result = country_summary.select("country", "first_signup_date")
