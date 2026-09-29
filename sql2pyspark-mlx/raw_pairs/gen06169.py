from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .filter(F.col("we.session_id").isin([8, 14, 15, 36]))
    .select(
        F.col("we.id"),
        F.col("we.duration_sec"),
        F.col("we.event_time"),
        F.col("s.country"),
        F.concat_ws("/", F.col("we.event_type"), F.col("we.page")).alias("event_type_page_key"),
    )
    .orderBy(F.col("event_time").desc())
)
