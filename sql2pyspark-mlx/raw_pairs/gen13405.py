from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") > "2024-03-03")
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"), "is_active")
    .agg(
        F.sum("age").alias("total_age"),
        F.count("*").alias("customer_count"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
    .orderBy(F.col("customer_count").desc(), "signup_month", "is_active")
    .limit(10)
)
