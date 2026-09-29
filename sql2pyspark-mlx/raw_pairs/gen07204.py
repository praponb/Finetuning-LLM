from pyspark.sql import functions as F

base = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2023-06-20").cast("date"))
    .withColumn("device_lower", F.lower("device"))
)

not_flagged = (
    base
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_anti")
)

matched = (
    eligible
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & F.col("s.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    matched
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.col("c.segment").alias("segment"),
        F.col("c.city").alias("city"),
        F.col("e.browser").alias("browser"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("e.subscription_id")).alias("min_subscription_id"),
        F.avg(F.col("e.monthly_fee")).alias("avg_monthly_fee"),
    )
)
