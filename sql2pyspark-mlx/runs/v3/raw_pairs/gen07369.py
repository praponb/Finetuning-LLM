from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("ss.avg_duration_sec"),
    )
    .orderBy("id", F.col("avg_duration_sec").desc())
)
