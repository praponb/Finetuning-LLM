from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(F.min("credit_limit").alias("min_credit_limit"))
    .filter(F.col("min_credit_limit") > 15500)
    .select("signup_month", "min_credit_limit")
)
