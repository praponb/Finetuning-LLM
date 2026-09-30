from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") <= "2025-04-01")
    .select(
        "id",
        "is_active",
        "email",
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
        F.date_format("signup_date", "MMM yyyy").alias("signup_date_label"),
    )
)
