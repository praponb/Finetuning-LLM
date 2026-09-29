from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") < "2024-10-22")
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.max("signup_date").alias("latest_signup_date"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("credit_limit")).otherwise(0)).alias("singapore_credit_limit"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
    .filter(F.col("total_credit_limit") < 25000)
    .drop("total_credit_limit")
)
