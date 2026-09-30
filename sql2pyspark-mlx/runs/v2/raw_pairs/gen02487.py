from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
    )
    .agg(
        F.countDistinct(F.col("s.monthly_fee")).alias("unique_plans"),
        F.sum(F.when(F.col("s.plan") == "premium", F.col("s.monthly_fee")).otherwise(0)).alias("premium_monthly_fee"),
    )
    .orderBy(F.col("premium_monthly_fee").desc(), "signup_month", "signup_quarter")
    .limit(10)
)
