from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.year(F.col("c.signup_date")) == 2023)
    .groupBy(F.col("c.id"), F.col("c.segment"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        F.round(F.avg(F.col("s.monthly_fee")), 2).alias("avg_monthly_fee"),
    )
    .filter(F.col("avg_monthly_fee") >= 21)
    .orderBy("id", "avg_monthly_fee", "segment")
    .limit(10)
)
