from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.filter(F.col("browser").isin(["Chrome", "Firefox", "Safari"])).alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left_anti")
    .filter(F.col("web_events.event_time") < "2023-10-01")
    .select(F.col("web_events.id"), F.col("web_events.duration_sec"))
    .orderBy("id", F.col("duration_sec").desc())
)
