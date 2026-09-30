from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter((F.col("credit_limit") >= 7500) & F.col("city").isin(["Berlin", "Bangkok", "Chiang Mai"]))
)

signup_month_summary = (
    filtered_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("country") == "JP", F.col("age")).otherwise(0)).alias("jp_age"),
    )
)

result = (
    signup_month_summary
    .filter(F.col("jp_age") >= 63)
    .select("signup_month", "jp_age")
    .orderBy(F.col("signup_month").desc(), F.col("jp_age").desc())
)
