from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.event_type").isin(["page_view", "click", "search"]) & F.col("s.browser").isin(["Safari", "Edge"]))
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("s.device"),
        F.col("s.customer_id"),
        F.col("s.country"),
    )
)
