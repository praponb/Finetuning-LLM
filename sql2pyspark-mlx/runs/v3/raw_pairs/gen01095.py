from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") != "Bangkok")
    .select(
        "id",
        "signup_date",
        "is_active",
        (F.col("age") * F.col("credit_limit")).alias("age_times_credit_limit"),
        (F.col("credit_limit") + F.col("age")).alias("credit_limit_plus_age"),
    )
)
