from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("email") > 14)
    .select(
        "segment",
        "is_active",
        "signup_date",
        (F.col("credit_limit") + F.col("age")).alias("credit_limit_plus_age"),
    )
    .orderBy(F.col("segment").desc())
)
