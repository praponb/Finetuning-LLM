from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").between("2024-08-01", "2025-05-29"))
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(F.max("credit_limit").alias("max_credit_limit"))
    .filter(F.col("max_credit_limit") > 15000)
)
