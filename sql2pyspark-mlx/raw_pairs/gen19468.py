from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "outer")
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("we.duration_sec"),
        F.col("we.page"),
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("s.device"),
    )
)
