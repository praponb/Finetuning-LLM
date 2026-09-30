from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        F.max("signup_date").alias("latest_signup_date"),
        F.max("age").alias("max_age"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
    .orderBy(F.col("max_credit_limit").desc())
)
