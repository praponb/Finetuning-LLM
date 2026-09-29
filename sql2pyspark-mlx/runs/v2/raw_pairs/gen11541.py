from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .groupBy(F.col("s.id"), F.col("s.device"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.sort_array(F.collect_set(F.col("we.event_type"))).alias("event_type_list"),
        (F.max(F.col("we.duration_sec")) - F.min(F.col("we.duration_sec"))).alias("duration_sec_range"),
    )
)
