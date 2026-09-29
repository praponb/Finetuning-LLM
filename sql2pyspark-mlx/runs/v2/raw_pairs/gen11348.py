from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "age",
        "credit_limit",
        "name",
        (F.col("credit_limit") + F.col("age")).alias("credit_limit_plus_age"),
        F.col("credit_limit").cast("int").alias("credit_limit_int"),
    )
)
