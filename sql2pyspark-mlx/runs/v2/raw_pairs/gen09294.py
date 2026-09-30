from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.filter(F.col("session_id").isin([2, 5, 18])).alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left_semi")
    .filter(F.col("sessions.started_at") < "2025-08-01")
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("sessions.device"),
    )
    .orderBy(F.col("device").desc())
)
