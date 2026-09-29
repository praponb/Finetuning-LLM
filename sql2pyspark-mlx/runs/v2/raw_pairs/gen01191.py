from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").between("2024-08-01", "2025-06-01"))
    .select("id", "country", "age")
    .subtract(
        df_customers
        .filter(F.col("signup_date").between("2023-07-01", "2024-12-01"))
        .select("id", "country", "age")
    )
)
