from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.year("signup_date") == 2023) & (F.col("credit_limit") < 11600) & (F.col("credit_limit") < 11800))
    .select(
        "id",
        "country",
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)
