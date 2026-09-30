from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("plan") == "basic")
    .withColumn("clean_plan", F.trim("plan"))
)

agg = (
    src
    .groupBy(
        F.when(F.col("monthly_fee") >= 260, "high").when(F.col("monthly_fee") >= 89, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
)

result = (
    agg
    .filter(F.col("avg_monthly_fee") >= 60)
    .select("monthly_fee_bucket", "cancelled_count", "avg_monthly_fee")
    .orderBy("cancelled_count", "monthly_fee_bucket")
    .limit(3)
)
