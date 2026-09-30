from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter((F.col("s.customer_id") == 9) | F.col("we.session_id").isin([7, 17]))
    .select(
        F.col("we.event_time"),
        F.col("we.duration_sec"),
        F.col("we.event_type"),
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.concat(F.col("s.browser"), F.lit("/"), F.col("s.country")).alias("browser_country"),
    )
)
