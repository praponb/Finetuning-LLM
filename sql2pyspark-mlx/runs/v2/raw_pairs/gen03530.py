from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("name").like("%FRANK%"))
    .groupBy("segment", F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0)).alias("singapore_count"),
        F.min("signup_date").alias("min_signup_date"),
    )
    .filter(F.col("singapore_count") < 39)
    .orderBy(F.col("min_signup_date").desc())
)
