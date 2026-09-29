from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("event_time") < "2023-04-01")
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/checkout", F.col("duration_sec")).otherwise(0)).alias("checkout_duration_sec"),
        F.min("event_time").alias("min_event_time"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(
        F.col("s.browser"),
        F.col("s.device"),
        F.col("ss.checkout_duration_sec"),
        F.col("ss.min_event_time"),
    )
    .orderBy(F.col("device").desc(), "browser")
)
