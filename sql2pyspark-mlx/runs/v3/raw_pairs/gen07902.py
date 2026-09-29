from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("t.cancelled_count"),
        F.col("t.plan_list"),
    )
    .orderBy("signup_date", F.col("id").desc())
    .limit(3)
)
