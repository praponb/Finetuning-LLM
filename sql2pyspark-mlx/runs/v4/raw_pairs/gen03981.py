from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.city") == "London")
    .groupBy(F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.max(F.col("s.monthly_fee")).alias("max_monthly_fee"),
        F.count(F.col("s.monthly_fee")).alias("monthly_fee_count"),
    )
    .filter(F.col("monthly_fee_count") > 5)
    .orderBy("signup_month")
    .limit(10)
)
