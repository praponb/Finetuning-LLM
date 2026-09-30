from pyspark.sql import functions as F

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-11-05 16:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.lower("event_type").alias("event_type_lower"),
        F.hour("event_time").alias("event_time_hour"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_filled"),
    )
)

enriched = (
    base
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.country"), F.col("s.browser"), F.col("s.device"))
)

matched_keys = (
    enriched
    .select("session_id", "page")
    .subtract(
        df_web_events
        .filter(~F.col("event_type").isin(["page_view", "search", "click"]))
        .select("session_id", "page")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.to_date("event_time") >= "2023-11-11")
    .withColumn("page_browser", F.concat("page", F.lit(", "), "browser"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("DIRECT")))
)

result = (
    cleaned
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        "event_type_lower",
        "event_time_hour",
        "browser",
        "device",
    )
    .orderBy(F.col("browser").desc())
)
