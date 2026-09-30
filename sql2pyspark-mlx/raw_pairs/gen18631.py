from pyspark.sql import functions as F

result = (
    df_customers
    .filter(((F.col("country") != "SG") & (F.col("signup_date") >= "2025-07-01")) | F.col("is_active"))
    .select("id", "name", "age", F.lower("city").alias("city_lower"))
)
