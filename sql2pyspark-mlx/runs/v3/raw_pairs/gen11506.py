from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "SMB")
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(F.count("credit_limit").alias("credit_limit_count"))
    .filter(F.col("credit_limit_count") < 5)
    .select("signup_month", "credit_limit_count")
)
