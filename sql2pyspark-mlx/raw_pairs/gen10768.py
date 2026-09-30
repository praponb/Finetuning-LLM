from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower(F.trim("email")) == "grace.kim@gmail.com")
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.countDistinct("credit_limit").alias("unique_segments"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("credit_limit")).otherwise(0)).alias("berlin_credit_limit"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
    )
    .orderBy(F.col("berlin_credit_limit").desc())
)
