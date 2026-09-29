from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "email",
        "credit_limit",
        F.length("country").alias("country_length"),
        (F.col("age") * F.col("credit_limit")).alias("age_times_credit_limit"),
    )
)
