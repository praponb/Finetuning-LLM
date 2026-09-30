from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_trunc("QUARTER", "start_date").alias("quarter_start"))
    .agg(
        F.percentile_approx("monthly_fee", 0.95).alias("monthly_fee_p95"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.percentile_approx("monthly_fee", 0.95).alias("monthly_fee_p95_2"),
        F.avg("monthly_fee").alias("avg_monthly_fee_2"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("monthly_fee_p95_2") < 1000000)
)
