from pyspark.sql import functions as F

joined = (
    df_sessions
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .filter(~F.col("p.is_active"))
    .select("s.id", "s.customer_id", "s.started_at", "s.country", F.col("p.city"))
)

valid = (
    joined
    .filter((F.col("started_at") > F.lit("2023-03-17 23:00:00").cast("timestamp")) & (F.col("started_at") >= "2024-08-05"))
    .select(
        "*",
        F.concat_ws("-", "country", "city").alias("country_city_key"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_clean"),
    )
)

matched = (
    valid
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.started_at") >= F.col("sub.start_date")) & (F.col("s.started_at") <= F.date_add(F.col("sub.end_date"), 3)), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

kept_keys = (
    matched
    .select("customer_id", "id")
    .exceptAll(
        df_sessions
        .filter(F.col("device").isin(["desktop", "mobile", "tablet"]))
        .select("customer_id", "id")
    )
)

verified = (
    matched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.segment"), F.col("p.country").alias("customer_country"))
)

valid_sessions = (
    with_dims
    .filter(F.col("plan").isin(["premium", "enterprise", "standard"]))
    .select(
        "*",
        F.length("country").alias("country_length"),
        F.lower("customer_country").alias("customer_country_lower"),
    )
)

eligible = (
    valid_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.duration_sec") > 340), "left_semi")
)

result = (
    eligible
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["customer_country", "customer_segment", "session_city"], ["customer_country", "customer_segment"], ["customer_country"], []],
        F.col("p.country").alias("customer_country"),
        F.col("p.segment").alias("customer_segment"),
        F.col("e.city").alias("session_city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("e.subscription_id")).alias("total_subscription_id"),
        F.max(F.col("e.monthly_fee")).alias("max_monthly_fee"),
        F.grouping_id().alias("grouping_level"),
    )
)
