from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.country"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        F.avg(F.col("s.monthly_fee")).alias("avg_monthly_fee"),
        F.count(F.col("s.monthly_fee")).alias("monthly_fee_count"),
    )
    .orderBy("country")
    .limit(20)
)
