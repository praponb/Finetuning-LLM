from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("email") < 10)
    .groupBy(F.quarter("signup_date").alias("signup_quarter"), "city")
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.count("*").alias("customer_count"),
    )
    .orderBy(F.col("customer_count").desc(), "signup_quarter", "city")
    .limit(5)
)
