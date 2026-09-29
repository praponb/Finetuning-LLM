from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .filter(F.col("status") == "PAUSED")
    .groupBy("customer_id")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.segment"),
        F.col("c.credit_limit"),
        F.col("t.monthly_fee_count"),
        F.col("t.monthly_fee_range"),
    )
    .orderBy("credit_limit")
)
