from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.max("event_time").alias("latest_event_time"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .filter(F.col("ss.unique_sessions") > 10)
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.country"),
        F.col("ss.latest_event_time"),
        F.col("ss.unique_sessions"),
    )
    .orderBy("country")
)
