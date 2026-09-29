from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-06-10 22:00:00").cast("timestamp")) & F.col("country").isNotNull() & F.col("customer_id").isin([7, 19, 32]))
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        F.when(F.col("device").isin(["mobile", "desktop"]), "Tier 1").otherwise("Other").alias("device_group"),
    )
)

kept_keys = (
    filtered
    .select("browser", "id")
    .subtract(
        df_sessions
        .filter(F.col("browser") == "Edge")
        .select("browser", "id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.started_at")), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") <= F.col("s.started_at")), "left_anti")
)

not_flagged_sessions = (
    not_flagged
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") <= F.col("s.started_at")), "left_semi")
)

not_flagged_sessions_2 = (
    not_flagged_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_anti")
)

summary = (
    not_flagged_sessions_2
    .groupBy(F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        F.count_if(F.col("country") == "JP").alias("jp_events"),
        F.max("started_at").alias("last_started_at"),
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_rows"),
    )
)

result = (
    summary
    .select("quarter_start", "safari_rows")
    .orderBy(F.col("quarter_start").desc())
)
