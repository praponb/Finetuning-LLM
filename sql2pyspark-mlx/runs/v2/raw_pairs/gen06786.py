from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city", F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(F.max("credit_limit").alias("max_credit_limit"))
    .orderBy("city")
)
