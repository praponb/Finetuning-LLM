from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["Berlin", "Tokyo", "London"]) & (F.col("signup_date").between("2024-08-06", "2025-05-08") | (F.col("credit_limit") >= 6200)))
    .agg(
        F.sum("age").alias("total_age"),
        F.count("credit_limit").alias("credit_limit_count"),
    )
)
