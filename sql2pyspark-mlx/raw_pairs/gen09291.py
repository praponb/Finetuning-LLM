from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.event_time") >= "2025-01-01")
    .select(
        F.col("s.browser"),
        F.col("s.device"),
        F.col("we.page"),
        F.col("we.event_time"),
    )
)
