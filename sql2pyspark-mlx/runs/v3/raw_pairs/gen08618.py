from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .groupBy("city", F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(F.count("*").alias("num_customers"))
    .filter(F.col("num_customers") >= 8)
)
