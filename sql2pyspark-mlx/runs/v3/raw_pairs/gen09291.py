from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("subscriptions.plan") == "basic")
    .groupBy(F.month(F.col("customers.signup_date")).alias("signup_month"))
    .agg(
        F.sort_array(F.collect_set(F.col("subscriptions.plan"))).alias("plan_list"),
        F.max(F.col("subscriptions.end_date")).alias("last_end_date"),
    )
    .orderBy("last_end_date", "signup_month")
    .limit(20)
)
