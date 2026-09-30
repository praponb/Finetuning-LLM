from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "signup_date",
        "credit_limit",
        (F.col("credit_limit") * F.col("age")).alias("credit_limit_times_age"),
    )
    .orderBy(F.col("id").desc())
)
