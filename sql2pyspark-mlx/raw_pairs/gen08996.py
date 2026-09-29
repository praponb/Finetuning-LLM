from pyspark.sql import functions as F

duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

matched_keys = (
    df_web_events
    .select("page", "event_type")
    .exceptAll(
        df_web_events
        .filter(F.col("session_id") == 13)
        .select("page", "event_type")
    )
)

kept_rows = (
    df_web_events
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.col("avg_web_event_duration_sec"),
    )
    .orderBy(F.col("duration_sec").desc_nulls_first())
)
