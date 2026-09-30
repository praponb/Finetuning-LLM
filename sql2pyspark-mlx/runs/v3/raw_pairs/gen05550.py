from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.to_date("started_at") < "2023-08-23").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .filter(F.col("we.event_type") == "click")
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("we.event_time"),
        F.col("we.event_type"),
    )
    .orderBy("event_time", "id")
    .limit(10)
)
