from pyspark.sql import functions as F

matched = (
    df_sessions
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.started_at") >= F.col("sub.start_date")) & (F.col("a.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    matched
    .groupBy(
        "subscription_id",
        F.when(F.col("subscription_id") >= 8, "LARGE").otherwise("SMALL").alias("subscription_id_tier"),
    )
    .agg(
        F.avg(F.when(F.col("country") == "TH", F.col("monthly_fee"))).alias("th_avg_monthly_fee"),
        F.max("started_at").alias("ended_at"),
    )
    .orderBy("th_avg_monthly_fee")
)
