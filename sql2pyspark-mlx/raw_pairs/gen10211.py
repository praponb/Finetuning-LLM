from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("session_stats"), F.col("sessions.id") == F.col("session_stats.session_id"), "left")
    .select(
        F.col("sessions.browser"),
        F.col("sessions.device"),
        F.col("session_stats.search_duration_sec"),
    )
)
