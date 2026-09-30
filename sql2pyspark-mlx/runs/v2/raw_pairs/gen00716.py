from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "outer")
    .filter(~F.col("customers.name").like("%Henry%"))
    .groupBy(
        F.date_trunc("MONTH", F.col("customers.signup_date")).alias("signup_month"),
        F.col("customers.country"),
    )
    .agg(
        F.max(F.col("subscriptions.monthly_fee")).alias("max_monthly_fee"),
        F.sum(F.when(F.col("subscriptions.plan") == "standard", 1).otherwise(0)).alias("standard_count"),
    )
    .orderBy("signup_month")
)
