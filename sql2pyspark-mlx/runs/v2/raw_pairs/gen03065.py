from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.is_active"), F.col("c.segment"))
    .agg(
        F.max(F.col("s.monthly_fee")).alias("max_monthly_fee"),
        F.sort_array(F.collect_set(F.col("s.status"))).alias("status_list"),
    )
    .orderBy("segment", F.col("max_monthly_fee").desc(), "is_active")
    .limit(5)
)
