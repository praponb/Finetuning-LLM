from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("segment") == "Consumer") & (F.col("age") >= 55))
    .select("email", "age", "credit_limit", F.lower("city").alias("city_lower"))
)
