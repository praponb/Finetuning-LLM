from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("status") == "ACTIVE"))
    .select(
        "*",
        F.trim("status").alias("clean_status"),
        (F.datediff(F.current_date(), "start_date") / 7).alias("weeks_since_start_date"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("monthly_fee") >= 196, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_tier"),
    )
    .agg(
        F.avg(F.when(F.col("plan") == "standard", F.col("monthly_fee"))).alias("standard_avg_monthly_fee"),
        F.count("*").alias("event_count"),
        F.count_if(F.col("status") == "EXPIRED").alias("expired_events"),
    )
    .orderBy(F.col("expired_events").desc())
)
