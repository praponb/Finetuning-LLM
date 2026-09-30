from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .filter(F.col("ss.max_duration_sec") >= 300)
    .select(F.col("s.browser"), F.col("ss.max_duration_sec"))
    .orderBy("browser", "max_duration_sec")
)
