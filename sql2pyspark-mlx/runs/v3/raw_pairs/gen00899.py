from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(~F.col("status").isin(["EXPIRED", "CANCELLED", "ACTIVE"]) & (F.col("customer_id").isin([4, 29, 35]) | (F.col("plan") == "enterprise")))
    .withColumn("status_plan_key", F.concat_ws("-", "status", "plan"))
)

subscriptions_stats = (
    recent_subscriptions
    .groupBy(
        F.quarter("end_date").alias("end_quarter"),
        F.year("end_date").alias("end_year"),
    )
    .agg(F.countDistinct("monthly_fee").alias("unique_plans"))
)

result = (
    subscriptions_stats
    .filter(F.col("unique_plans") > 8)
    .orderBy("unique_plans", "end_quarter", "end_year")
    .limit(5)
)
