from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left")
    .groupBy(
        F.month(F.col("customers.signup_date")).alias("signup_month"),
        F.col("customers.country"),
    )
    .agg(
        F.count(F.col("subscriptions.monthly_fee")).alias("monthly_fee_count"),
        (F.max(F.col("subscriptions.monthly_fee")) - F.min(F.col("subscriptions.monthly_fee"))).alias("monthly_fee_range"),
    )
    .filter(F.col("monthly_fee_count") >= 2)
)
