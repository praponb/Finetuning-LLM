from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@company.co") & ~F.col("is_active"))
    .cube("is_active", F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.min("signup_date").alias("first_signup_date"))
    .orderBy("signup_month")
)
