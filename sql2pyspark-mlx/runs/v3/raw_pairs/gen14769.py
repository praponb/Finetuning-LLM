from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(
        F.col("c.is_active"),
        F.month(F.col("c.signup_date")).alias("signup_month"),
    )
    .agg(F.count("*").alias("subscription_count"))
    .filter(F.col("subscription_count") > 2)
)
