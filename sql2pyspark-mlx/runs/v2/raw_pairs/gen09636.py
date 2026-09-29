from pyspark.sql import functions as F

result = (
    df_customers
    .filter(((F.col("credit_limit") >= 8600) & (F.col("city") == "London")) | ~F.col("is_active"))
    .groupBy("country")
    .agg(F.max("signup_date").alias("last_signup_date"))
)
