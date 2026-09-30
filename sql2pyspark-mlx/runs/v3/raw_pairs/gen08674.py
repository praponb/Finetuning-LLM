from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") < "2025-02-24")
    .select(
        "id",
        "signup_date",
        "age",
        F.floor("credit_limit").alias("credit_limit_whole"),
    )
)
