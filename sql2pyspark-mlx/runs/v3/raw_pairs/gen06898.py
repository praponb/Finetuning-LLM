from pyspark.sql import functions as F

recent_subscriptions = df_subscriptions.filter(F.month("start_date").isin([1, 5, 7]))

agg = (
    recent_subscriptions
    .groupBy("plan")
    .agg(
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_count"),
        F.countDistinct("monthly_fee").alias("unique_statuss"),
    )
)

result = (
    agg
    .filter(F.col("unique_statuss") < 8)
    .select("plan", "paused_count", "unique_statuss")
    .orderBy("paused_count", "plan")
    .limit(50)
)
