from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .filter(F.col("web_events.session_id").isin([3, 19, 20, 38]) | F.col("sessions.browser").isin(["Edge", "Firefox", "Safari"]))
    .select(
        F.col("web_events.id"),
        F.col("web_events.duration_sec"),
        F.col("web_events.event_time"),
        F.col("web_events.event_type"),
        F.col("sessions.browser"),
        F.col("sessions.device"),
    )
    .orderBy(F.col("id").desc())
)
