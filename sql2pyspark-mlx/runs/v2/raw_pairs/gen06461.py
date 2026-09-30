from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["Berlin", "London"]) & F.upper("email").like("%VY@%") & (F.col("city") == "Tokyo"))
    .select(
        "id",
        "is_active",
        F.least("credit_limit", "age").alias("least_credit_limit_age"),
    )
)
