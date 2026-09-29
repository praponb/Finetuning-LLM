from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(F.sort_array(F.collect_set("event_type")).alias("event_type_list"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(F.col("s.id"), F.col("s.device"), F.col("ss.event_type_list"))
)
