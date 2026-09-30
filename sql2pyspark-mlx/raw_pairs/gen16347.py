from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter((F.to_date(F.col("we.event_time")) < "2024-10-01") & (F.col("we.event_time") >= "2023-03-01"))
    .select(
        F.col("s.device"),
        F.col("we.session_id"),
        F.col("we.event_type"),
        F.col("we.duration_sec"),
        F.lower(F.col("we.page")).alias("page_lower"),
    )
)
