from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.row_count") >= 9)
    .select(F.col("s.id"), F.col("s.browser"), F.col("t.row_count"))
)
