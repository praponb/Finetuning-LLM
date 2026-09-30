from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 71, "high").when(F.col("age") >= 25, "mid").otherwise("low").alias("age_bucket"),
        F.date_format("signup_date", "yyyy-MM").alias("signup_month"),
    )
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.count("*").alias("customer_count"),
    )
    .filter(F.col("min_credit_limit") < 9000)
    .select("age_bucket", "signup_month", "min_credit_limit", "customer_count")
    .orderBy(F.col("age_bucket").desc())
)
