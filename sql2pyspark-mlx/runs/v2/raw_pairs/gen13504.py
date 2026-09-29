from pyspark.sql import functions as F

filtered_subscriptions = df_subscriptions.filter(F.col("start_date") < "2024-09-02")

subscriptions_stats = (
    filtered_subscriptions
    .groupBy("plan")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
    )
)

result = subscriptions_stats.select("plan", "pct_cancelled")
