from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.filter(F.col("duration_sec").isNotNull()).alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left_anti")
    .filter(F.col("sessions.country") == "TH")
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("sessions.device"),
        F.col("sessions.browser"),
    )
)
