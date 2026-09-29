from pyspark.sql import functions as F

base = (
    df_customers
    .filter((F.col("credit_limit") < 14400) & (F.col("city") == "Singapore") & (F.col("city") != "Bangkok"))
)

signup_month_summary = (
    base
    .groupBy(
        F.date_trunc("MONTH", "signup_date").alias("signup_month"),
        F.when(F.col("age") >= 63, "high").when(F.col("age") >= 33, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.avg("age").alias("avg_age"))
)

result = signup_month_summary.select("signup_month", "age_bucket", "avg_age")
