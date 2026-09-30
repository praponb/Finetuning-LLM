from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .filter(F.col("status").isin(["PAUSED", "EXPIRED"]))
    .groupBy("customer_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"))
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "left")
    .select(F.col("c.name"), F.col("sbc.plan_values"))
)
