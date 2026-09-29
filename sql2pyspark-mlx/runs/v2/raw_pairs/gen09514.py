from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.is_active"), F.col("c.country"))
    .agg(
        F.max(F.col("s.monthly_fee")).alias("max_monthly_fee"),
        (F.max(F.col("s.monthly_fee")) - F.min(F.col("s.monthly_fee"))).alias("monthly_fee_range"),
    )
    .orderBy(F.col("country").desc())
)
