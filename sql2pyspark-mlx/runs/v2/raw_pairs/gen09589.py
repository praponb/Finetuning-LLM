from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter((F.col("s.customer_id") == 39) & (F.col("we.page") != "/product"))
    .select(
        F.col("s.started_at"),
        F.col("we.duration_sec"),
        F.col("we.event_time"),
        F.col("we.session_id"),
        F.ceil(F.col("we.duration_sec")).alias("duration_sec_whole"),
    )
    .orderBy(F.col("session_id").desc(), F.col("started_at").desc())
)
