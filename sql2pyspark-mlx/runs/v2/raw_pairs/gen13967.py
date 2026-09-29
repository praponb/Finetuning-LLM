from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("event_time") >= "2023-10-02")
    .groupBy("session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "left")
    .select(
        F.col("s.device"),
        F.col("webs.total_duration_sec"),
        F.col("webs.min_duration_sec"),
    )
    .orderBy("device", "min_duration_sec")
)
