from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("duration_sec") >= 167)
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(F.col("s.id"), F.col("s.browser"), F.col("t.event_type_values"))
)
