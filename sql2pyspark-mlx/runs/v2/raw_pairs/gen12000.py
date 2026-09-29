from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 100)
    .groupBy("customer_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.email"),
        F.col("c.signup_date"),
        F.col("cs.plan_values"),
    )
    .orderBy(F.col("plan_values").desc())
)
