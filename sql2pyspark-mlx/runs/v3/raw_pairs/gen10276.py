from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("duration_sec").between(160, 266))
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "left")
    .select(
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("webs.checkout_count"),
        F.col("webs.max_duration_sec"),
    )
)
