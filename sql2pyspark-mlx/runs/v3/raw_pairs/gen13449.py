from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("s.status").isin(["EXPIRED", "PAUSED", "CANCELLED"]))
    .groupBy(F.col("c.id"), F.col("c.name"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        (F.max(F.col("s.monthly_fee")) - F.min(F.col("s.monthly_fee"))).alias("monthly_fee_range"),
    )
    .orderBy(F.col("id").desc(), F.col("monthly_fee_range").desc())
)
