from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") >= "2023-07-22")
    .groupBy("segment")
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        F.min("credit_limit").alias("min_credit_limit"),
    )
    .filter(F.col("min_credit_limit") >= 15600)
)
