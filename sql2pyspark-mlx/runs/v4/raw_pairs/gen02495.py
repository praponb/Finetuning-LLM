from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").isNull() & F.col("email").like("%@outlook.com"))
    .select(
        "id",
        "credit_limit",
        "city",
        F.col("credit_limit").cast("int").alias("credit_limit_int"),
    )
)
