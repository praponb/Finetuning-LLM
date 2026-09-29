from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .select(
        F.col("we.duration_sec"),
        F.col("we.page"),
        F.col("we.session_id"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.date_trunc("MONTH", F.col("s.started_at")).alias("started_at_period"),
    )
)
