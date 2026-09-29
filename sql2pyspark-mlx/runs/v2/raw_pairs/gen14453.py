from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.customer_id").isNull() & F.col("s.browser").isin(["Safari", "Firefox"]))
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("we.duration_sec"),
        F.col("we.page"),
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("s.country"),
    )
)
