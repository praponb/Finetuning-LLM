from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("S"), F.col("s.id") == F.col("S.session_id"), "left")
    .filter(F.col("S.unique_event_types") >= 4)
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("S.event_type_values"),
        F.col("S.unique_event_types"),
    )
    .orderBy("browser")
)
