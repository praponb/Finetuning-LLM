from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.hour("event_time").between(6, 14))
    .groupBy("session_id")
    .agg(
        F.countDistinct("duration_sec").alias("unique_event_types"),
        F.count("*").alias("num_web_events"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.num_web_events") >= 4)
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.country"),
        F.col("ss.unique_event_types"),
        F.col("ss.num_web_events"),
    )
    .orderBy("browser")
)
