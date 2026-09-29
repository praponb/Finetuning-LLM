from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "US")
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(F.round(F.avg("age"), 2).alias("avg_age"))
    .filter(F.col("avg_age") >= 61)
)
