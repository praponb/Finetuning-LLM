from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"))
    .filter(F.col("berlin_count") > 3)
)
