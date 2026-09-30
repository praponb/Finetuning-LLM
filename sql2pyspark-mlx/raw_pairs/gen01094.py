from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") >= 5500)
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.count("age").alias("age_count"),
    )
    .orderBy(F.col("age_count").desc())
)
