from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.event_type").isin(["purchase", "search"]) & F.col("s.country").isin(["DE", "TH"]))
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("s.device"),
        F.col("s.started_at"),
        F.length(F.col("we.page")).alias("page_length"),
    )
)
