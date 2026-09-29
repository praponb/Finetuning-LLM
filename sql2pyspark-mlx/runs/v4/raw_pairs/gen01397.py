from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .select(
        F.col("s.country"),
        F.col("s.device"),
        F.col("webs.duration_sec_count"),
        F.col("webs.unique_event_types"),
    )
    .orderBy(F.col("unique_event_types").desc())
)
