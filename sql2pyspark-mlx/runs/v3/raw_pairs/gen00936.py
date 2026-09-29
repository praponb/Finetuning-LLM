from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("s.status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
        (F.max(F.col("s.monthly_fee")) - F.min(F.col("s.monthly_fee"))).alias("monthly_fee_range"),
    )
    .orderBy(F.col("monthly_fee_range").desc())
)
