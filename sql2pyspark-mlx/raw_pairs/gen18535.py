from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-05-08 00:00:00").cast("timestamp"))
    .select(
        "*",
        F.to_date("event_time").alias("event_time_date"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

matched_keys = (
    cleaned
    .select("id", "session_id")
    .intersectAll(
        df_web_events
        .filter(~F.col("page").isin(["/cart", "/checkout"]))
        .select("id", "session_id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "left")
    .filter(F.col("b.started_at") < "2024-01-01")
    .select(
        "s.session_id",
        "s.event_type",
        "s.event_time",
        "s.page_filled",
        F.col("b.browser"),
    )
)

joined_web_events = (
    joined
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.browser").alias("session_browser"))
)

with_dims = (
    joined_web_events
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .select("e.session_id", "e.event_type", "e.session_browser", F.col("s.device"))
)

result = (
    with_dims
    .select(
        "session_id",
        "event_type",
        "session_browser",
        "device",
        F.when(F.col("session_browser").rlike("^(Edge|Firefox)$"), "GROUP_A").when(F.col("session_browser").rlike("^(Chrome|Safari)$"), "CORE").otherwise("OTHER_ZONE").alias("session_browser_zone"),
    )
    .orderBy("session_browser")
)
