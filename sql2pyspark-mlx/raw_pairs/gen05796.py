from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("ct.avg_monthly_fee"),
        F.col("ct.min_monthly_fee"),
    )
)
