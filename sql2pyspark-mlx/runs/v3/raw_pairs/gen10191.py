from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") >= "2023-07-13")
    .groupBy("country")
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.count("credit_limit").alias("credit_limit_count"),
    )
    .filter(F.col("us_count") > 5)
    .orderBy("country", F.col("credit_limit_count").desc())
)
