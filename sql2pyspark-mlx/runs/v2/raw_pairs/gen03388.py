from pyspark.sql import functions as F

base = (
    df_customers
    .filter((F.col("signup_date") > "2024-09-01") | F.col("segment").isin(["Consumer", "Corporate"]))
)

signup_month_summary = (
    base
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.count("*").alias("total_count"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
)

result = (
    signup_month_summary
    .orderBy(F.col("signup_month").desc(), F.col("max_credit_limit").desc())
)
