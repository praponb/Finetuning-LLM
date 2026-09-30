from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & (F.col("country") == "JP"))
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"), "is_active")
    .agg(
        F.min("signup_date").alias("first_signup_date"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
    )
    .filter(F.col("tokyo_count") > 2)
    .orderBy(F.col("tokyo_count").desc(), "signup_month", "is_active")
    .limit(3)
)
