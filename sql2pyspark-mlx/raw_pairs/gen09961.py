from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("duration_sec").isNotNull())
    .groupBy("session_id")
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.cnt") >= 7)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("t.cnt"),
        F.col("t.purchase_duration_sec"),
    )
)
