from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("S"), F.col("s.id") == F.col("S.session_id"), "inner")
    .select(
        F.col("s.country"),
        F.col("s.browser"),
        F.col("S.event_type_values"),
        F.col("S.max_duration_sec"),
    )
    .orderBy("max_duration_sec")
)
