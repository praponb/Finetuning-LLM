from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") > 9600) & F.col("is_active") & (F.col("country") == "UK"))
    .select("signup_date", F.lower("segment").alias("segment_lower"))
    .orderBy("signup_date")
)
