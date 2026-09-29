from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        F.sum(F.when(F.col("country") == "US", F.col("credit_limit")).otherwise(0)).alias("us_credit_limit"),
        (F.max("age") - F.min("age")).alias("age_range"),
    )
)
