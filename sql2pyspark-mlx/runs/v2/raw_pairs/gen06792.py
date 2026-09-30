from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.col("c.city"),
        F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"),
    )
    .agg(
        F.count(F.when(F.col("s.status") == "PAUSED", 1)).alias("paused_count"),
        F.countDistinct(F.col("s.monthly_fee")).alias("unique_plans"),
    )
    .orderBy("unique_plans")
)
