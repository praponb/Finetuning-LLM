from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.filter(F.col("event_time") < "2023-09-01").alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left_semi")
    .filter(F.col("sessions.browser").isin(["Chrome", "Firefox"]))
    .select(
        F.col("sessions.id"),
        F.col("sessions.device"),
        F.col("sessions.customer_id"),
    )
)
