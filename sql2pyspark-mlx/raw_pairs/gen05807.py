from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.total_monthly_fee") >= 610)
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("t.avg_monthly_fee"),
        F.col("t.total_monthly_fee"),
    )
    .orderBy("total_monthly_fee", "id")
    .limit(3)
)
