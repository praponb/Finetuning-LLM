from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("duration_sec") > 351)
    .groupBy("session_id")
    .agg(F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .filter(F.col("t.purchase_count") >= 6)
    .select(F.col("s.id"), F.col("s.browser"), F.col("t.purchase_count"))
    .orderBy("purchase_count")
)
