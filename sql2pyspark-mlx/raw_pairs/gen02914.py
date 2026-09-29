from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(11400, 15500) & (F.col("age") < 41))
    .groupBy("is_active")
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.min("signup_date").alias("min_signup_date"),
    )
)
