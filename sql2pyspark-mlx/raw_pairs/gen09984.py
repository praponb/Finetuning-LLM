from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.max("signup_date").alias("latest_signup_date"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
    .orderBy("signup_month", F.col("latest_signup_date").desc())
)
