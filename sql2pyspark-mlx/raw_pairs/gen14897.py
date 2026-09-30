from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.month("signup_date").alias("signup_month"), "is_active")
    .agg(
        F.max("signup_date").alias("last_signup_date"),
        F.sum(F.when(F.col("segment") == "Consumer", F.col("credit_limit")).otherwise(0)).alias("consumer_credit_limit"),
    )
    .filter(F.col("consumer_credit_limit") < 36)
)
