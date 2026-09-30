from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "is_active",
        "city",
        (F.col("age") * F.col("credit_limit")).alias("age_times_credit_limit"),
    )
)
