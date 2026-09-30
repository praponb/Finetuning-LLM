from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("session_id").isin([19, 27]))
    .groupBy("session_id")
    .agg(F.count(F.when(F.col("event_type") == "click", 1)).alias("click_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(F.col("s.country"), F.col("ss.click_count"))
    .orderBy("country")
)
