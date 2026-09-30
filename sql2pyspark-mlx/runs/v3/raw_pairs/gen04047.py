from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["US", "DE", "UK"]))
    .select(
        "credit_limit",
        "signup_date",
        "city",
        F.col("credit_limit").cast("int").alias("credit_limit_int"),
    )
    .orderBy(F.col("credit_limit").desc(), F.col("city").desc())
)
