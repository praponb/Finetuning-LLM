from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(~F.col("page").isin(["/product", "/search", "/cart"]))
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("S"), F.col("s.id") == F.col("S.session_id"), "inner")
    .filter(F.col("S.avg_duration_sec") >= 200)
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("S.event_type_values"),
        F.col("S.avg_duration_sec"),
    )
)
