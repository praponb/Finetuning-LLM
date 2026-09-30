from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .filter((F.col("web_events.page") != "/product") & F.hour(F.col("sessions.started_at")).between(9, 15))
    .select(
        F.col("sessions.device"),
        F.col("web_events.duration_sec"),
        F.round((F.col("web_events.duration_sec") * 1.07), 2).alias("duration_sec_adjusted"),
    )
)
