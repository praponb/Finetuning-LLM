from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter((F.col("s.customer_id") == 6) | (F.col("s.started_at") < "2025-06-01"))
    .select(
        F.col("s.browser"),
        F.col("s.device"),
        F.col("we.event_time"),
        F.col("we.session_id"),
    )
)
