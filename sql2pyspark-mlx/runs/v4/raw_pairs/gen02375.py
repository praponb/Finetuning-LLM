from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("S"), F.col("s.id") == F.col("S.session_id"), "inner")
    .select(F.col("s.id"), F.col("s.started_at"), F.col("S.purchase_count"))
)
