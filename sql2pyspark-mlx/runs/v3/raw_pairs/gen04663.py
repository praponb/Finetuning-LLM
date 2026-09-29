from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active", F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.count("credit_limit").alias("credit_limit_count"),
        F.count("*").alias("num_customers"),
    )
    .orderBy(F.col("signup_month").desc())
)
