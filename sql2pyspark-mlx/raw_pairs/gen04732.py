from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["New York", "Chiang Mai", "Bangkok"]) & F.col("signup_date").between("2024-10-01", "2025-10-01"))
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
    )
)
