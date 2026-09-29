from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.browser").isin(["Chrome", "Safari", "Firefox"]) & (F.col("s.started_at") >= "2025-02-01"))
    .select(
        F.col("we.page"),
        F.col("we.event_type"),
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("s.started_at"),
    )
    .orderBy("started_at", "event_type")
)
