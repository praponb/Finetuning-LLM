from pyspark.sql import functions as F

filtered_customers = df_customers.filter(F.col("age").between(41, 49))

customers_stats = (
    filtered_customers
    .groupBy("is_active", F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"))
)

result = (
    customers_stats
    .filter(F.col("tokyo_count") >= 7)
    .select("is_active", "signup_month", "tokyo_count")
    .orderBy(F.col("tokyo_count").desc())
)
