from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-06-07", "2024-06-08"))
    .withColumn("plan_title", F.initcap(F.trim("plan")))
)

subscriptions_stats = (
    valid_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 238, "high").when(F.col("monthly_fee") >= 79, "mid").otherwise("low").alias("monthly_fee_bucket"),
        "plan",
    )
    .agg(F.count("*").alias("subscription_count"))
)

result = subscriptions_stats.orderBy("subscription_count", "plan")
