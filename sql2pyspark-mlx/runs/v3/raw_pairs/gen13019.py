from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.plan").isin(["standard", "premium"]))
    .groupBy(F.col("c.country"))
    .agg(
        F.count(F.when(F.col("s.status") == "ACTIVE", 1)).alias("active_count"),
        F.max(F.col("s.start_date")).alias("latest_start_date"),
    )
    .orderBy(F.col("latest_start_date").desc())
)
