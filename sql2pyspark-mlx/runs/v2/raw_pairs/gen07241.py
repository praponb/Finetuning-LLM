from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active", F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.count("age").alias("age_count"))
    .filter(F.col("age_count") > 8)
)
