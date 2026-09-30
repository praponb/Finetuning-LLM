from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("s.customer_id").isin([2, 8, 17, 19]))
    .select(
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("s.browser"),
        F.col("we.duration_sec"),
        F.col("we.event_time"),
    )
)
