from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(
        F.sort_array(F.collect_set(F.col("s.status"))).alias("status_list"),
        F.round(((100 * F.sum(F.when(F.col("s.status") == "ACTIVE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_active"),
    )
)
