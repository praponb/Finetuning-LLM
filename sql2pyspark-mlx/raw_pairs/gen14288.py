from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("segment") == "Consumer", F.col("credit_limit")).otherwise(0)).alias("consumer_credit_limit"),
    )
    .filter(F.col("consumer_credit_limit") > 35)
    .select("signup_month", "consumer_credit_limit")
)
