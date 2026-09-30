from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter(F.col("plan").isin(["premium", "basic", "standard"]) & ~F.col("status").isin(["CANCELLED", "PAUSED"]))
    .withColumn("clean_status", F.trim("status"))
)

subscriptions_stats = (
    filtered_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 246, "high").when(F.col("monthly_fee") >= 67, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.countDistinct("monthly_fee").alias("unique_statuss"))
)

result = subscriptions_stats
