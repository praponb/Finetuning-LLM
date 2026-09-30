from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .filter(F.hour(F.col("web_events.event_time")).between(8, 13))
    .select(
        F.col("web_events.id"),
        F.col("web_events.page"),
        F.col("web_events.duration_sec"),
        F.col("sessions.country"),
    )
    .orderBy("page", "id")
    .limit(20)
)
