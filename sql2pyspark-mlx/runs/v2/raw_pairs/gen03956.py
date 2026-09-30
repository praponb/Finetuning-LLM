from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "signup_date",
        "name",
        "country",
        F.round((F.col("credit_limit") / F.col("age")), 2).alias("credit_limit_per_age"),
    )
)
