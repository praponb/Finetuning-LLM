from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").between("2023-05-27", "2025-03-01"))
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(F.min("age").alias("min_age"))
    .filter(F.col("min_age") >= 48)
    .select("signup_month", "min_age")
    .orderBy(F.col("signup_month").desc())
)
