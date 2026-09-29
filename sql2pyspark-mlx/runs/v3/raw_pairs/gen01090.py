from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.country") != "JP")
    .groupBy(F.col("c.id"), F.col("c.segment"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.plan")))).alias("plan_values"),
        F.avg(F.col("s.monthly_fee")).alias("avg_monthly_fee"),
    )
    .filter(F.col("avg_monthly_fee") > 100)
    .orderBy("segment")
)
