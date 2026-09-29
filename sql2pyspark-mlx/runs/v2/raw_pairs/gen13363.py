from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("W"), F.col("s.id") == F.col("W.session_id"), "inner")
    .filter(F.col("W.min_duration_sec") > 350)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("W.event_type_values"),
        F.col("W.min_duration_sec"),
    )
)
