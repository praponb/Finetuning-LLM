from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.duration_sec_count") > 4)
    .select(
        F.col("s.browser"),
        F.col("s.country"),
        F.col("ss.event_type_values"),
        F.col("ss.duration_sec_count"),
    )
)
