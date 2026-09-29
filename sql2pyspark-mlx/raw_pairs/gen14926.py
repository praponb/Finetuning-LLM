from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.lower(F.col("c.name")).like("%vy %"))
    .groupBy(F.col("c.id"), F.col("c.city"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        F.round(F.stddev(F.col("s.monthly_fee")), 2).alias("stddev_monthly_fee"),
        F.sort_array(F.collect_set(F.col("s.plan"))).alias("plan_list"),
    )
    .filter(F.col("subscription_count") >= 4)
    .orderBy("subscription_count")
)
