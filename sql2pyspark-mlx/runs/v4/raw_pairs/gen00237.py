from pyspark.sql import functions as F

result = (
    df_customers
    .filter(((F.col("age") <= 48) & F.col("is_active")) | (F.col("signup_date") > "2025-06-17"))
    .groupBy("segment")
    .agg(F.count("credit_limit").alias("credit_limit_count"))
    .orderBy(F.col("credit_limit_count").desc(), "segment")
    .limit(3)
)
