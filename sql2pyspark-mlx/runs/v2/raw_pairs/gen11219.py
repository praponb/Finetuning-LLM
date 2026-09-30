from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.min("start_date").alias("min_start_date"),
        F.count("*").alias("subscription_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(F.col("c.city"), F.col("t.min_start_date"), F.col("t.subscription_count"))
    .orderBy("city")
)
