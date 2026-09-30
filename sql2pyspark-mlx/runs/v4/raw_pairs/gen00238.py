from pyspark.sql import functions as F

duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

remaining_keys = (
    df_web_events
    .select("page")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec") >= 329)
        .select("page")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.col("avg_web_event_duration_sec"),
    )
    .orderBy(F.col("avg_web_event_duration_sec").desc(), F.col("duration_sec").desc())
)
