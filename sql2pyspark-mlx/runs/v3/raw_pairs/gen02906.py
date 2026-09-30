from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .filter(F.col("t.stddev_duration_sec") >= 55)
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("t.page_list"),
        F.col("t.stddev_duration_sec"),
    )
)
