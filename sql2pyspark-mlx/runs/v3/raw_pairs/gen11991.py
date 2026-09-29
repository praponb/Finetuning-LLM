from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "segment",
        "credit_limit",
        F.date_trunc("YEAR", "signup_date").alias("signup_date_period"),
    )
)
