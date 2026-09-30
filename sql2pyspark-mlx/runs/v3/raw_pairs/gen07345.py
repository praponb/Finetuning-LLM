from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter((F.col("s.started_at") < "2025-09-01") & (F.col("s.browser") != "Safari"))
    .select(
        F.col("s.device"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("we.event_type"),
        F.col("we.event_time"),
    )
    .orderBy("event_type")
)
