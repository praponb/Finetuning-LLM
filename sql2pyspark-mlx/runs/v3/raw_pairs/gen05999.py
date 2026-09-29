from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .filter(F.col("plan") != "premium")
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("num_subscriptions"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .filter(F.col("ct.num_subscriptions") > 7)
    .select(
        F.col("c.city"),
        F.col("c.name"),
        F.col("ct.num_subscriptions"),
        F.col("ct.avg_monthly_fee"),
    )
)
