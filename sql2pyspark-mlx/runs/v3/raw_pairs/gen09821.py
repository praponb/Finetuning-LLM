from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.max("signup_date").alias("last_signup_date"),
        F.max("signup_date").alias("latest_signup_date"),
        F.count("age").alias("age_count"),
    )
    .filter(F.col("age_count") < 5)
    .drop("age_count")
)
