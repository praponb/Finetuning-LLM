from pyspark.sql import functions as F

matched_keys = (
    df_web_events
    .select("page", "session_id")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec").isNotNull())
        .select("page", "session_id")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("session_id", "page", "event_time", "duration_sec")
    .orderBy(F.col("event_time").desc_nulls_last())
)
