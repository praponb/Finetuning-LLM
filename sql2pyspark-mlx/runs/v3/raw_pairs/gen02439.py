from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") <= 9000) & F.col("email").like("%@startup.io"))
    .select(
        "id",
        "signup_date",
        "segment",
        F.substring("email", 1, 3).alias("email_prefix"),
    )
)
