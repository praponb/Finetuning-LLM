from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.min("signup_date").alias("min_signup_date"),
        F.sum(F.when(F.col("segment") == "Corporate", 1).otherwise(0)).alias("corporate_count"),
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
    )
    .orderBy("credit_limit_range")
)
