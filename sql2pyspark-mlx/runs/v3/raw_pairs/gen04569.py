from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .filter(F.col("status") == "ACTIVE")
    .groupBy("customer_id")
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("cs.stddev_monthly_fee"),
        F.col("cs.monthly_fee_count"),
    )
    .orderBy(F.col("monthly_fee_count").desc())
)
