from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNotNull())
    .groupBy("segment", F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("city") == "London", F.col("credit_limit")).otherwise(0)).alias("london_credit_limit"),
    )
    .filter(F.col("london_credit_limit") >= 37)
)
