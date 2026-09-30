from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.lower(F.trim("email")) == "liam@outlook.com") & F.col("email").like("%@gmail.com"))
    .groupBy("segment", F.month("signup_date").alias("signup_month"))
    .agg((F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"))
)
