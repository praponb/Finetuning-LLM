from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("plan")).alias("plan_list"))
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .select(F.col("c.country"), F.col("c.segment"), F.col("sbc.plan_list"))
    .orderBy("country", "segment")
)
