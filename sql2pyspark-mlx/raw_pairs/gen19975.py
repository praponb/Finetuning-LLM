from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.segment").isNull())
    .groupBy(F.col("c.city"))
    .agg(
        F.min(F.col("s.monthly_fee")).alias("min_monthly_fee"),
        (F.max(F.col("s.monthly_fee")) - F.min(F.col("s.monthly_fee"))).alias("monthly_fee_range"),
    )
    .filter(F.col("monthly_fee_range") >= 21)
    .orderBy(F.col("city").desc())
)
