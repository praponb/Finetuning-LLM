from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() | (F.col("end_date") <= "2024-09-01"))
    .withColumn("plan_group", F.when(F.col("plan").isin(["standard", "premium"]), "Group A").otherwise("Other"))
)

end_quarter_summary = (
    src
    .groupBy(F.quarter("end_date").alias("end_quarter"), "plan")
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
)

result = (
    end_quarter_summary
    .select("end_quarter", "plan", "monthly_fee_count")
    .orderBy("plan", "end_quarter")
    .limit(5)
)
