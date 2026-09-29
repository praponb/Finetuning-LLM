from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(F.sort_array(F.collect_set("page")).alias("page_list"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(F.col("s.device"), F.col("s.started_at"), F.col("t.page_list"))
    .orderBy(F.col("device").desc())
)
