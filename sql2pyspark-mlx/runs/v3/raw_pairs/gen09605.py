from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter((F.to_date(F.col("s.started_at")) >= "2024-05-29") & (F.col("s.browser") == "Chrome"))
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.session_id"),
        F.col("we.event_time"),
        F.col("s.browser"),
        F.col("s.country"),
    )
    .orderBy(F.col("session_id").desc())
)
