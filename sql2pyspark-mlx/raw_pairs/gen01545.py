from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.min("event_time").alias("first_event_time"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "left")
    .select(
        F.col("s.browser"),
        F.col("s.country"),
        F.col("webs.duration_sec_range"),
        F.col("webs.first_event_time"),
    )
    .orderBy("browser")
)
