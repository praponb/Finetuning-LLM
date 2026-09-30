from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select(
        "id",
        "signup_date",
        "credit_limit",
        "email",
        F.col("is_active").cast("int").alias("is_active_flag"),
        F.initcap(F.trim("email")).alias("email_title"),
    )
)
