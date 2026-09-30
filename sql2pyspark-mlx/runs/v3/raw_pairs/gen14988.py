from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .filter(~F.col("we.event_type").isin(["page_view", "search"]))
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("s.device"),
        F.col("s.country"),
        F.col("s.browser"),
    )
)
