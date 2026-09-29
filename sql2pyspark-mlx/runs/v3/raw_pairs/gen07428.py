from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.max_monthly_fee") > 70)
    .select(
        F.col("c.is_active"),
        F.col("cs.monthly_fee_range"),
        F.col("cs.max_monthly_fee"),
    )
    .orderBy(F.col("max_monthly_fee").desc())
)
