from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") >= 15500)
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(F.min("signup_date").alias("min_signup_date"))
)
