from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0)).alias("premium_count"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .select(F.col("c.email"), F.col("ct.premium_count"), F.col("ct.avg_monthly_fee"))
    .orderBy(F.col("avg_monthly_fee").desc())
)
