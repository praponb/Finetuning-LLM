from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .select(
        F.col("we.session_id"),
        F.col("we.event_time"),
        F.col("we.page"),
        F.col("s.device"),
        F.col("s.country"),
        F.col("s.browser"),
    )
)
