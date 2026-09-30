from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0)).alias("basic_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .select(
        F.col("c.email"),
        F.col("c.country"),
        F.col("sbc.basic_count"),
        F.col("sbc.status_list"),
    )
    .orderBy("email")
)
