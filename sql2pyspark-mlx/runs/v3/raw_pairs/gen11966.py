from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count("*").alias("web_event_count"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .filter(F.col("t.web_event_count") > 4)
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("t.web_event_count"),
        F.col("t.event_type_list"),
    )
    .orderBy(F.col("id").desc())
)
