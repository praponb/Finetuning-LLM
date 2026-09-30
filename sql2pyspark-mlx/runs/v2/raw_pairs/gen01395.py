from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") >= "2023-07-01") & F.col("plan").isin(["enterprise", "premium"]) & F.col("customer_id").isNotNull())
    .groupBy("status")
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("avg_monthly_fee") < 100)
    .orderBy("avg_monthly_fee", "status")
    .limit(10)
)
