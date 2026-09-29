from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("we.session_id").isin([7, 35]))
    .select(
        F.col("s.started_at"),
        F.col("we.page"),
        F.col("we.event_time"),
        F.col("we.duration_sec"),
    )
    .orderBy("started_at")
)
