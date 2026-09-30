from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left")
    .groupBy(F.col("customers.city"))
    .agg(
        F.count(F.when(F.col("subscriptions.status") == "ACTIVE", 1)).alias("active_count"),
        F.count(F.when(F.col("subscriptions.status") == "PAUSED", 1)).alias("paused_count"),
    )
)
