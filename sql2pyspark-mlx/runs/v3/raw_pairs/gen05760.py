from pyspark.sql import functions as F

src = df_subscriptions.filter(F.col("status") == "PAUSED")

plan_group_summary = (
    src
    .groupBy(
        F.when(F.col("plan").isin(["basic", "premium"]), "tier_1").otherwise("other").alias("plan_group"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
)

result = (
    plan_group_summary
    .orderBy(F.col("cancelled_count").desc(), "plan_group")
    .limit(20)
)
