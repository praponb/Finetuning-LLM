from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id"), F.col("s.subscription_id_2")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

latest = (
    df_sessions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_window = (
    latest
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

matched = (
    with_window
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.started_at") >= F.col("sub.start_date")) & (F.col("a.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

with_last = (
    matched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.started_at",
        "s.subscription_id",
        "s.plan",
        "s.subscription_id_2",
        "s.subscription_plan",
        "s.subscription_monthly_fee",
        F.col("c.event_time"),
    )
)

base = (
    with_last
    .filter((F.col("started_at") > F.to_timestamp(F.lit("2023-05-07 12:00:00"))) & F.col("plan").isNotNull())
    .select(
        "*",
        F.when(F.col("subscription_id") >= 9, "High").when(F.col("subscription_id") >= 5, "Medium").otherwise("Low").alias("subscription_id_level"),
        F.coalesce("subscription_plan", F.lit("OTHER")).alias("resolved_subscription_plan"),
    )
)

base_sessions = (
    base
    .filter(F.col("plan").isNotNull() & ~F.col("subscription_plan").isin(["standard", "basic"]))
    .select(
        "*",
        F.coalesce("subscription_id_2", F.lit(0)).alias("subscription_id_2_filled"),
    )
)

result = (
    base_sessions
    .groupBy(
        F.when(F.col("subscription_monthly_fee") >= 250, "PREMIUM").otherwise("STANDARD").alias("subscription_monthly_fee_tier"),
    )
    .agg(
        F.countDistinct("subscription_plan").alias("distinct_subscription_plans"),
        F.min("started_at").alias("first_started_at"),
        F.min("event_time").alias("first_event_time"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
    )
    .orderBy(F.col("distinct_subscription_plans").desc_nulls_first())
)
