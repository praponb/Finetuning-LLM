from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") <= 4700) & ((F.col("age") != 56) | (F.col("segment") != "Corporate")))
    .select(
        "credit_limit",
        "signup_date",
        "email",
        (F.col("credit_limit") - F.col("age")).alias("credit_limit_minus_age"),
    )
    .orderBy(F.col("credit_limit").desc())
)
