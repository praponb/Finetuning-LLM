from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.quarter("signup_date").alias("signup_quarter"),
        F.date_format("signup_date", "yyyy-MM").alias("signup_month"),
    )
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        F.count("credit_limit").alias("credit_limit_count"),
    )
    .filter(F.col("credit_limit_count") > 5)
)
