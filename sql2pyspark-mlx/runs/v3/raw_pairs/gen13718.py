from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").between("2023-09-04", "2025-01-01"))
    .select(
        "id",
        "is_active",
        "credit_limit",
        "email",
        F.trim("segment").alias("clean_segment"),
    )
)
