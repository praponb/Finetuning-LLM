from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("duration_sec").isNotNull())
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.max("event_time").alias("last_event_time"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("SessionStats"), F.col("sessions.id") == F.col("SessionStats.session_id"), "inner")
    .select(
        F.col("sessions.id"),
        F.col("sessions.browser"),
        F.col("sessions.device"),
        F.col("SessionStats.checkout_count"),
        F.col("SessionStats.last_event_time"),
    )
    .orderBy(F.col("last_event_time").desc())
)
