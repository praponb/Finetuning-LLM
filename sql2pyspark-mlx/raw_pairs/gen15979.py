from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "standard") | ~F.col("status").isin(["ACTIVE", "CANCELLED"]))
    .groupBy(
        F.when(F.col("plan").isin(["standard", "enterprise"]), "group_a").otherwise("other").alias("plan_group"),
        "status",
    )
    .agg(
        F.count("*").alias("subscription_count"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") > 2)
    .drop("total_count")
    .orderBy("status", "plan_group")
    .limit(10)
)
