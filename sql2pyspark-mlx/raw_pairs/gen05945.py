from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("event_time") >= "2024-05-01")
    .groupBy("session_id")
    .agg(F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(F.col("s.id"), F.col("s.browser"), F.col("ss.checkout_count"))
    .orderBy(F.col("checkout_count").desc())
)
