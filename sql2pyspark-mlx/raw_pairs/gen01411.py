from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("we.event_type"),
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("s.customer_id"),
    )
    .orderBy(F.col("session_id").desc())
)
