from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.sum(F.when(F.col("event_type") == "search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("S"), F.col("s.id") == F.col("S.session_id"), "inner")
    .filter(F.col("S.search_duration_sec") >= 25)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("S.event_type_values"),
        F.col("S.search_duration_sec"),
    )
)
