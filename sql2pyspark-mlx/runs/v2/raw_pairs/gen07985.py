from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.city") == "Singapore")
    .groupBy(F.col("c.segment"))
    .agg(
        F.sort_array(F.collect_set(F.col("s.status"))).alias("status_list"),
        F.max(F.col("s.monthly_fee")).alias("max_monthly_fee"),
    )
    .orderBy("segment")
)
