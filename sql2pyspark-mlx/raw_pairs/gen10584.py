from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 17000, "high").when(F.col("credit_limit") >= 6900, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.date_trunc("MONTH", "signup_date").alias("signup_month"),
    )
    .agg(
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
        F.min("signup_date").alias("min_signup_date"),
    )
    .filter(F.col("credit_limit_range") > 49)
    .orderBy(F.col("signup_month").desc())
)
