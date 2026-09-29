from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("page") != "/home")
    .groupBy("session_id")
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("WebEventsBySession"), F.col("sessions.id") == F.col("WebEventsBySession.session_id"), "left")
    .filter(F.col("WebEventsBySession.avg_duration_sec") > 75)
    .select(F.col("sessions.country"), F.col("WebEventsBySession.avg_duration_sec"))
)
