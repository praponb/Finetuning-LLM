from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .select(
        F.col("web_events.id"),
        F.col("web_events.duration_sec"),
        F.col("sessions.country"),
    )
    .orderBy(F.col("duration_sec").desc(), "id")
    .limit(5)
)
