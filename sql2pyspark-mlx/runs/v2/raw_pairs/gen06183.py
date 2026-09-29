from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(F.min("signup_date").alias("first_signup_date"))
    .orderBy("segment")
)
