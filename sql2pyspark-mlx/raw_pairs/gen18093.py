from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("s.plan") == "standard", F.col("s.monthly_fee")).otherwise(0)).alias("standard_monthly_fee"),
        F.count(F.col("s.monthly_fee")).alias("monthly_fee_count"),
    )
)
