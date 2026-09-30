from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .groupBy(F.col("c.segment"))
    .agg(
        F.count(F.when(F.col("s.status") == "ACTIVE", 1)).alias("active_count"),
        F.min(F.col("s.monthly_fee")).alias("min_monthly_fee"),
    )
    .orderBy("segment")
)
