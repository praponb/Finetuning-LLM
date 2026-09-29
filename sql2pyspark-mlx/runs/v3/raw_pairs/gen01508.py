from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.session_id") == 21)
    .select(
        F.col("we.event_time"),
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("s.started_at"),
    )
    .orderBy("event_time", F.col("device").desc())
)
