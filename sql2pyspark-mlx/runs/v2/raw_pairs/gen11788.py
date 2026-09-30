from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy("segment", F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("age")).otherwise(0)).alias("singapore_age"),
        F.sum(F.when(F.col("country") == "US", F.col("credit_limit")).otherwise(0)).alias("us_credit_limit"),
    )
)
