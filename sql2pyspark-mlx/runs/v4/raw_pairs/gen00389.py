from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.page").isNotNull())
    .select(
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("we.event_type"),
        F.col("we.duration_sec"),
        F.col("we.event_time"),
    )
    .orderBy("event_type")
)
