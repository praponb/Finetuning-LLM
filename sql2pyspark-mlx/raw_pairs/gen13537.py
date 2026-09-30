from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

base = (
    df_sessions
    .filter(F.col("started_at") > F.to_timestamp(F.lit("2023-05-03 17:00:00")))
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        F.trim("device").alias("clean_device"),
        F.coalesce("country", F.lit("N/A")).alias("resolved_country"),
    )
)

matched = (
    base
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_latest = (
    matched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.page").alias("web_event_page"),
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

matched_keys = (
    with_latest
    .select("customer_id", "device")
    .subtract(
        df_sessions
        .filter(F.col("browser") != "Safari")
        .select("customer_id", "device")
    )
)

verified = (
    with_latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select(
        "s.customer_id",
        "s.device",
        "s.resolved_country",
        "s.subscription_id",
        "s.monthly_fee",
        "s.web_event_page",
        "s.web_event_event_type",
        F.col("p.credit_limit"),
        F.col("p.age"),
        F.col("p.email"),
    )
)

joined = (
    enriched
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.signup_date") > "2025-02-26")
    .select("src.*", F.col("p.segment"), F.col("p.name"), F.col("p.country"))
)

result = (
    joined
    .withColumn("monthly_fee_tier", F.when(F.col("monthly_fee") >= 219, "LARGE").otherwise("SMALL"))
    .withColumn("subscription_id_tier", F.when(F.col("subscription_id") >= 8, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["monthly_fee_tier", "web_event_event_type", "subscription_id_tier"], ["monthly_fee_tier", "web_event_event_type"]],
        "monthly_fee_tier",
        "web_event_event_type",
        "subscription_id_tier",
    )
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.percentile_approx("subscription_id", [0.25, 0.5, 0.75]).alias("subscription_id_quartiles"),
        F.percentile_approx("age", 0.95).alias("age_p95"),
    )
    .filter(F.col("avg_monthly_fee") > 0)
)
