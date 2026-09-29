from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-06-24 03:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
    )
)

joined = (
    cleaned
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.customer_id").isNotNull())
    .select(
        "e.id",
        "e.session_id",
        "e.duration_sec",
        F.col("s.country"),
        F.col("s.browser"),
    )
)

enriched = (
    joined
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.session_id",
        "a.country",
        "a.browser",
        F.col("p.device"),
        F.col("p.browser").alias("session_browser"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.browser").alias("session_browser_2"))
)

report = (
    with_dims
    .select(
        "id",
        "session_id",
        "country",
        "browser",
        "device",
        "session_browser",
        "session_browser_2",
        F.when(F.col("device").rlike("^(desktop|tablet)$"), "EXTENDED").when(F.col("device").rlike("^(mobile)$"), "CORE").otherwise("GROUP_A").alias("device_zone"),
    )
)

result = (
    report
    .select("id", "country", "browser", "device", "session_browser_2", "device_zone")
    .orderBy(F.col("id").desc())
)
