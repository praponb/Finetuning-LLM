from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter((F.col("signup_date") < "2024-05-08") | (F.col("signup_date") < "2025-05-01"))
    .withColumn("clean_city", F.trim("city"))
)

agg = (
    recent_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.min("age").alias("min_age"))
)

result = agg.orderBy(F.col("min_age").desc(), "signup_month")
