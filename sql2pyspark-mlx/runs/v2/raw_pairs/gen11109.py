from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.is_active"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("s.plan") == "enterprise", 1).otherwise(0))) / F.count("*")), 1).alias("pct_enterprise"),
        F.count(F.when(F.col("s.status") == "EXPIRED", 1)).alias("expired_count"),
    )
    .orderBy(F.col("expired_count").desc(), "is_active")
    .limit(50)
)
