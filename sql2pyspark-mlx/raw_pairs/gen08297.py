from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"))
    .filter(F.col("tokyo_count") < 21)
    .orderBy("signup_month")
)
