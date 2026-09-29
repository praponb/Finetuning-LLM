from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0))) / F.count("*")), 1).alias("pct_tokyo"),
        F.min("signup_date").alias("first_signup_date"),
    )
)
