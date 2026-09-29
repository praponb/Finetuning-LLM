from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("session_id").isin([3, 31]))
    .groupBy("session_id")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)

result = (
    df_sessions.alias("sessions")
    .join(session_summary.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.min_duration_sec") > 270)
    .select(
        F.col("sessions.id"),
        F.col("sessions.device"),
        F.col("sessions.country"),
        F.col("t.min_duration_sec"),
    )
)
