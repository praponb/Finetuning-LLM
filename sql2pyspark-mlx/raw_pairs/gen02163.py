from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(F.min("credit_limit").alias("min_credit_limit"))
    .orderBy("signup_month", "segment")
    .limit(5)
)
