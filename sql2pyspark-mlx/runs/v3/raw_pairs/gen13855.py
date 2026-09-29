from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["TH", "UK"]))
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.min("signup_date").alias("first_signup_date"),
        F.max("age").alias("max_age"),
    )
    .filter(F.col("max_age") < 50)
    .select("signup_month", "first_signup_date", "max_age")
)
