from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.max(F.col("s.end_date")).alias("last_end_date"),
        F.sum(F.when(F.col("s.status") == "CANCELLED", F.col("s.monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
    )
)
