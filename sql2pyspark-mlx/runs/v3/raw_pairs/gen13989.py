from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "age",
        "signup_date",
        "is_active",
        F.date_trunc("week", "signup_date").alias("signup_date_period"),
    )
)
