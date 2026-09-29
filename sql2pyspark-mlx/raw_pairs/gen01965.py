from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .groupBy(
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
        F.col("c.is_active"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("s.plan") == "basic", 1).otherwise(0))) / F.count("*")), 1).alias("pct_basic"),
        F.count(F.col("s.monthly_fee")).alias("monthly_fee_count"),
    )
    .orderBy("monthly_fee_count", "signup_month", "is_active")
    .limit(1)
)
