from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.year("signup_date") == 2023)
    .select(
        "id",
        "name",
        "credit_limit",
        "email",
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
    )
)
