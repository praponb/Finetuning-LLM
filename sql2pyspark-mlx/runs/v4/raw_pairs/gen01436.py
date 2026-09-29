from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@company.co"))
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.min("signup_date").alias("min_signup_date"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.sum(F.when(F.col("segment") == "Consumer", 1).otherwise(0)).alias("consumer_count"),
    )
)
