from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .filter(F.month("end_date").isin([2, 5, 12]))
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0))) / F.count("*")), 1).alias("pct_standard"),
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .filter(F.col("C.pct_standard") >= 33)
    .select(
        F.col("c.email"),
        F.col("c.is_active"),
        F.col("C.pct_standard"),
        F.col("C.plan_values"),
    )
    .orderBy(F.col("email").desc())
)
