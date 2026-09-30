from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("duration_sec") == 301)
    .groupBy("session_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .filter(F.col("ss.row_count") > 6)
    .select(F.col("s.country"), F.col("s.browser"), F.col("ss.row_count"))
    .orderBy(F.col("row_count").desc())
)
