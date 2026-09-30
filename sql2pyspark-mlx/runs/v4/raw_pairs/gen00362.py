from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull())
    .groupBy("is_active")
    .agg(
        F.count("credit_limit").alias("credit_limit_count"),
        F.min("signup_date").alias("first_signup_date"),
    )
)
