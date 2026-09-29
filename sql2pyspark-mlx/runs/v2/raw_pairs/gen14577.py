from pyspark.sql import functions as F

base = df_customers.filter(F.col("city") == "Chiang Mai")

agg = (
    base
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.sum("age").alias("total_age"),
        F.min("signup_date").alias("first_signup_date"),
    )
)

result = agg
