from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .filter(F.col("plan") == "enterprise")
    .groupBy("customer_id")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.segment"),
        F.col("c.is_active"),
        F.col("t.min_monthly_fee"),
    )
    .orderBy(F.col("id").desc())
)
