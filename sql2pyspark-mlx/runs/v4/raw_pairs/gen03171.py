from pyspark.sql import functions as F

remaining_keys = (
    df_web_events
    .select("session_id", "event_type")
    .intersectAll(
        df_web_events
        .filter(F.col("duration_sec") != 301)
        .select("session_id", "event_type")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "session_id", "event_type", "page", "event_time", "duration_sec")
    .orderBy(F.col("event_time").desc(), "page")
)
