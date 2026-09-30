from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "city",
        (F.col("age") + F.col("credit_limit")).alias("age_plus_credit_limit"),
    )
)
