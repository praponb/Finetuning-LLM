from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("page") != "/cart")
    .groupBy("session_id")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("W"), F.col("s.id") == F.col("W.session_id"), "inner")
    .filter(F.col("W.duration_sec_range") >= 55)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("W.duration_sec_range"),
        F.col("W.avg_duration_sec"),
    )
    .orderBy("id")
)
