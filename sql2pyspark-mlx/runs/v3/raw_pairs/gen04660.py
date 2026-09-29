from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .groupBy(F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.count(F.col("s.monthly_fee")).alias("monthly_fee_count"),
        F.min(F.col("s.start_date")).alias("min_start_date"),
    )
)
