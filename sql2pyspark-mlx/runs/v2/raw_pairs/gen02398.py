from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.event_type") != "page_view")
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("s.browser"),
        F.col("s.device"),
        F.hour(F.col("we.event_time")).alias("event_time_hour"),
    )
)
