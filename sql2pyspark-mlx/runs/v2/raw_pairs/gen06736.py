from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("web_event_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.customer_id"),
        F.col("ss.cnt"),
        F.col("ss.web_event_count"),
    )
)
