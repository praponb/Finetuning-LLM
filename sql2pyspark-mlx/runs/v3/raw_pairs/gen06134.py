from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter((F.col("we.event_type") == "page_view") | (F.to_date(F.col("s.started_at")) >= "2023-03-04"))
    .select(
        F.col("we.event_time"),
        F.col("we.page"),
        F.col("we.duration_sec"),
        F.col("s.customer_id"),
    )
    .orderBy(F.col("event_time").desc(), F.col("duration_sec").desc())
)
