from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("s.device").isin(["desktop", "tablet", "mobile"]) & (F.col("we.event_time") < "2023-08-01"))
    .select(
        F.col("s.browser"),
        F.col("we.event_type"),
        F.col("we.page"),
        F.col("we.duration_sec"),
    )
    .orderBy(F.col("duration_sec").desc())
)
