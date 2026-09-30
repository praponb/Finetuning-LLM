from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("we.session_id"),
        F.col("s.country"),
        F.col("s.customer_id"),
    )
)
