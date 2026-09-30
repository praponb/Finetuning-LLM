from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter((F.col("s.device") == "desktop") & (F.col("we.event_type") == "purchase"))
    .select(
        F.col("we.id"),
        F.col("we.duration_sec"),
        F.col("we.event_time"),
        F.col("we.session_id"),
        F.col("s.customer_id"),
        F.col("s.browser"),
    )
    .orderBy(F.col("id").desc())
)
