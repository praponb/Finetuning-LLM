from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"), "city")
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.count("*").alias("num_customers"),
    )
)
