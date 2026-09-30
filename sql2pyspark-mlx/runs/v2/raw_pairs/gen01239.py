from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("session_id").isin([5, 19]))
    .groupBy("session_id")
    .agg(
        F.countDistinct("duration_sec").alias("unique_event_types"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .filter(F.col("webs.total_duration_sec") >= 900)
    .select(
        F.col("s.browser"),
        F.col("webs.unique_event_types"),
        F.col("webs.total_duration_sec"),
    )
    .orderBy(F.col("unique_event_types").desc(), F.col("total_duration_sec").desc())
)
