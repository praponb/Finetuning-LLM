from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("c.country") != "DE")
    .groupBy(F.col("c.id"), F.col("c.city"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        F.count(F.when(F.col("s.status") == "PAUSED", 1)).alias("paused_count"),
    )
    .orderBy("city", "id")
    .limit(10)
)
