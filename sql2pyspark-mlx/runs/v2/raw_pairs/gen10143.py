from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.avg_monthly_fee") > 67)
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("c.country"),
        F.col("cs.avg_monthly_fee"),
    )
    .orderBy(F.col("avg_monthly_fee").desc())
)
