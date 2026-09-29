from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 110)
    .groupBy("customer_id")
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    df_customers.alias("customers")
    .join(subscriptions_by_customer.alias("t"), F.col("customers.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("customers.email"),
        F.col("customers.age"),
        F.col("t.avg_monthly_fee"),
        F.col("t.status_list"),
    )
)
