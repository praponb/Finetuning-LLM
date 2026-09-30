from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([6, 11, 14]) | F.col("status").isin(["PAUSED", "ACTIVE", "CANCELLED"]))
    .groupBy(
        "status",
        F.when(F.col("plan").isin(["standard", "basic"]), "priority").otherwise("other").alias("plan_group"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee")).otherwise(0)).alias("expired_monthly_fee"),
    )
    .orderBy("status", "plan_group")
    .limit(50)
)
