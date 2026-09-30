from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.hour("started_at").between(12, 19)).alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .filter(F.to_date(F.col("we.event_time")) >= "2025-01-01")
    .select(F.col("we.id"), F.col("we.page"), F.col("we.event_type"))
    .orderBy(F.col("page").desc(), "event_type")
)
