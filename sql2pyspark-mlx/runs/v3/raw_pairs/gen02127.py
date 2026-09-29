from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_subscriptions.alias("subscriptions"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("subscriptions.customer_id").isNull())
    .groupBy(F.col("customers.country"))
    .agg(
        F.count(F.col("subscriptions.id")).alias("subscription_count"),
        F.sort_array(F.collect_set(F.col("subscriptions.status"))).alias("status_list"),
        F.round(F.avg(F.col("subscriptions.monthly_fee")), 2).alias("avg_monthly_fee"),
    )
)
