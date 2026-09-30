from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter(F.col("start_date") <= "2025-07-03")
    .withColumn("monthly_fee_scaled", F.round((F.col("monthly_fee") * 1.1), 2))
)

agg = (
    filtered_subscriptions
    .groupBy(
        F.when(F.col("plan").isin(["premium", "basic"]), "group_a").otherwise("other").alias("plan_group"),
    )
    .agg(F.avg("monthly_fee").alias("avg_monthly_fee"))
)

result = (
    agg
    .filter(F.col("avg_monthly_fee") >= 170)
    .orderBy("avg_monthly_fee", "plan_group")
    .limit(3)
)
