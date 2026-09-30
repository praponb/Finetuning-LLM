from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull() & (F.col("credit_limit") < 13200))
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.max("age").alias("max_age"),
    )
)
