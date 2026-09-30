from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.max("signup_date").alias("latest_signup_date"),
    )
    .filter(F.col("min_credit_limit") >= 9800)
    .select("signup_quarter", "min_credit_limit", "latest_signup_date")
    .orderBy(F.col("min_credit_limit").desc(), "signup_quarter")
    .limit(3)
)
