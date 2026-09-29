from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.is_active"),
        F.col("c.country"),
        F.col("t.active_count"),
        F.col("t.status_list"),
    )
    .orderBy(F.col("country").desc())
)
