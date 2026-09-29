from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .groupBy(F.col("c.segment"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        F.round(F.avg(F.col("s.monthly_fee")), 2).alias("avg_monthly_fee"),
    )
)
