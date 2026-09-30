from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("duration_sec").isNull())
    .groupBy("session_id")
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .filter(F.col("t.avg_duration_sec") >= 65)
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("t.avg_duration_sec"),
    )
)
