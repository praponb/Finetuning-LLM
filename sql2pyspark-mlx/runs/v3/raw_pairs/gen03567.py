from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"), "is_active")
    .agg(
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.round(((100 * F.sum(F.when(F.col("city") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
    )
    .filter(F.col("avg_credit_limit") >= 12900)
)
