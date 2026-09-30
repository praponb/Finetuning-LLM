from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .groupBy(F.col("c.is_active"), F.col("c.country"))
    .agg(
        F.avg(F.col("s.monthly_fee")).alias("avg_monthly_fee"),
        F.sort_array(F.collect_set(F.col("s.status"))).alias("status_list"),
    )
    .orderBy("country")
)
