from pyspark.sql import functions as F

base = (
    df_customers
    .filter(~((F.year("signup_date") == 2025) & (F.col("is_active") | (F.col("credit_limit") <= 9300))))
    .withColumn("days_since_signup_date", F.datediff(F.current_date(), "signup_date"))
)

customers_stats = (
    base
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(F.min("days_since_signup_date").alias("min_days_since_signup_date"))
)

result = (
    customers_stats
    .select("signup_month", "min_days_since_signup_date")
    .orderBy("signup_month", F.col("min_days_since_signup_date").desc())
)
