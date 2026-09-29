from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.event_type").isin(["search", "page_view", "add_to_cart"]))
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.duration_sec"),
        F.col("we.session_id"),
        F.col("s.device"),
        F.col("s.customer_id"),
        F.col("s.started_at"),
    )
    .orderBy("device")
)
