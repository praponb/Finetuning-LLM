from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("credit_limit")).otherwise(0)).alias("berlin_credit_limit"),
    )
    .orderBy(F.col("max_credit_limit").desc(), "signup_quarter")
    .limit(3)
)
