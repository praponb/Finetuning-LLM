from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("ss.last_event_time"),
        F.col("ss.page_list"),
    )
)
