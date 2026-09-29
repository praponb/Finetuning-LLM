from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.stddev_monthly_fee") >= 51)
    .select(
        F.col("c.id"),
        F.col("c.email"),
        F.col("c.is_active"),
        F.col("t.stddev_monthly_fee"),
        F.col("t.plan_list"),
    )
)
