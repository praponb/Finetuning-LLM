from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("page").isin(["/home", "/product"]))
    .groupBy("session_id")
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(
        F.col("s.device"),
        F.col("ss.first_event_time"),
        F.col("ss.max_duration_sec"),
    )
)
