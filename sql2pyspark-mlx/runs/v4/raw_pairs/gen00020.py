from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("c.is_active"))
    .groupBy(F.col("c.id"), F.col("c.email"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        F.sum(F.col("s.monthly_fee")).alias("total_monthly_fee"),
    )
    .orderBy("email")
)
