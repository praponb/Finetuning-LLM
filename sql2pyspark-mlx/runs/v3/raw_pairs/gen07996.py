from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "signup_date",
        "credit_limit",
        "is_active",
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
    )
)
