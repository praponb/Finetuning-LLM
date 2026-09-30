from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy(F.year("started_at")).orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("plan").orderBy(F.col("duration_sec").desc(), "id", "subscription_id")
w_recent = Window.orderBy("subscription_id_share", "id", "subscription_id")
w6 = Window.orderBy(F.col("monthly_fee_sum_30d").desc(), "id", "subscription_id")

valid = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-05-03 19:00:00").cast("timestamp")) & ~F.col("device").isin(["mobile", "desktop"]))
    .withColumn("weeks_since_started_at", (F.datediff(F.current_date(), "started_at") / 7))
    .withColumn("country_group", F.when(F.col("country").isin(["UK", "DE"]), "Core").otherwise("Other"))
)

with_last = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.browser",
        "s.started_at",
        "s.country",
        F.col("c.duration_sec"),
    )
)

matched = (
    with_last
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.started_at") >= F.col("sub.start_date")) & (F.col("a.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

enriched = (
    matched
    .select(
        "id",
        "started_at",
        "duration_sec",
        "subscription_id",
        "plan",
        F.round((F.col("subscription_id") / F.sum("subscription_id").over(w2)), 4).alias("subscription_id_share"),
        F.sum("monthly_fee").over(w3).alias("monthly_fee_sum_30d"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "duration_sec",
        "subscription_id",
        "subscription_id_share",
        "monthly_fee_sum_30d",
    )
)

enriched_sessions = (
    deduped
    .filter(F.col("monthly_fee_sum_30d").between(338, 1035))
    .select(
        "id",
        "duration_sec",
        "subscription_id",
        F.ntile(5).over(w_recent).alias("subscription_id_share_bucket"),
        F.row_number().over(w6).alias("seq_no"),
        F.lead("duration_sec", 1, 0).over(w_recent).alias("next_duration_sec"),
    )
)

summary = (
    enriched_sessions
    .groupBy(
        F.when(F.col("duration_sec") >= 461, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.round(F.avg("subscription_id_share_bucket"), 2).alias("avg_subscription_id_share_bucket"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.percentile_approx("seq_no", 0.5).alias("seq_no_p50"),
        F.round(F.avg("next_duration_sec"), 2).alias("avg_next_duration_sec"),
        F.percentile_approx("subscription_id", 0.9).alias("subscription_id_p90"),
    )
    .filter(F.col("seq_no_p50") > 0)
)

result = (
    summary
    .select("duration_sec_tier", "avg_next_duration_sec", "subscription_id_p90")
    .orderBy(F.col("duration_sec_tier").asc_nulls_last(), "avg_next_duration_sec")
    .limit(10)
)
