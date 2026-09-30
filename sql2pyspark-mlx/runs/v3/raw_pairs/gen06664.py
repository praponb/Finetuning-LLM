from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .filter(F.col("web_events.duration_sec") > 328)
    .groupBy(F.col("sessions.country"))
    .agg(F.count(F.col("web_events.duration_sec")).alias("duration_sec_count"))
)
