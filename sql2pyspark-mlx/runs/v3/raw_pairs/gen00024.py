from pyspark.sql import functions as F

recent_subscriptions = df_subscriptions.filter(F.col("plan").isin(["premium", "enterprise", "basic"]))

agg = (
    recent_subscriptions
    .groupBy(
        F.when(F.col("plan").isin(["enterprise", "premium"]), "priority").otherwise("other").alias("plan_group"),
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
)

result = (
    agg
    .select("plan_group", "plan_list")
    .orderBy("plan_group")
    .limit(50)
)
