from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") > "2023-11-01") & (F.col("signup_date").between("2023-11-01", "2024-12-01") | ~F.col("is_active")))
    .select(
        "country",
        "age",
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
    )
    .orderBy(F.col("age").desc())
)
