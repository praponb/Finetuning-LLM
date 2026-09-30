from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") < 8100)
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.max("signup_date").alias("last_signup_date"),
        F.round(((100 * F.sum(F.when(F.col("city") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
    )
)
