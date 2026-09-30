from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(F.col("s.device"), F.col("s.started_at"), F.col("t.checkout_count"))
    .orderBy("device")
)
