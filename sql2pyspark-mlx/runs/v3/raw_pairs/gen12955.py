from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.id"), F.col("c.name"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        F.max(F.col("s.monthly_fee")).alias("max_monthly_fee"),
    )
    .filter(F.col("max_monthly_fee") > 190)
    .orderBy("subscription_count", "max_monthly_fee", "id", "name")
    .limit(3)
)
