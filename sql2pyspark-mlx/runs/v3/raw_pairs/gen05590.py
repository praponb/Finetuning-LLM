from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .select(
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("we.session_id"),
        F.col("we.event_time"),
    )
    .orderBy(F.col("customer_id").desc())
)
