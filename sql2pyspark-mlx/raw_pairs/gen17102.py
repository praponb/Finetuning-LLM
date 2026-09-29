from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.country").isin(["UK", "TH"]) & F.col("we.event_type").isin(["purchase", "click"]))
    .select(
        F.col("s.device"),
        F.col("s.country"),
        F.col("s.started_at"),
        F.col("we.duration_sec"),
    )
)
