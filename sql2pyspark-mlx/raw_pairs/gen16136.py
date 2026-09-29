from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter(F.col("browser").isNotNull() & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.substring("device", 1, 4).alias("device_prefix"),
        F.coalesce("browser", F.lit("NONE")).alias("browser_filled"),
    )
)

with_window = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.started_at") >= F.col("sub.start_date")) & (F.col("s.started_at") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

attributed = (
    with_window
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

base = (
    attributed
    .filter(F.col("browser").isNotNull() & (F.col("started_at") >= "2023-05-01"))
    .select(
        "id",
        "browser",
        "browser_filled",
        "subscription_id",
        "plan",
        "monthly_fee",
        "subscription_id_2",
        "subscription_plan",
        "subscription_monthly_fee",
        (F.col("monthly_fee") * F.col("subscription_monthly_fee")).alias("monthly_fee_times_subscription_monthly_fee"),
    )
)

result = (
    base
    .groupBy("browser")
    .pivot("subscription_plan", ["premium", "enterprise"])
    .agg(F.sum("subscription_id_2"))
)
