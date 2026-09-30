from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") <= 5500)
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.count("credit_limit").alias("credit_limit_count"),
    )
    .orderBy(F.col("max_credit_limit").desc())
)
