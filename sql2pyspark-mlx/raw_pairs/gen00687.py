from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .select(
        F.col("web_events.id"),
        F.col("web_events.page"),
        F.col("web_events.event_time"),
        F.col("sessions.device"),
        F.trim(F.col("web_events.event_type")).alias("clean_event_type"),
    )
)
