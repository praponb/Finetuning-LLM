from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.duration_sec").isNull())
    .select(
        F.col("s.started_at"),
        F.col("we.session_id"),
        F.col("we.duration_sec"),
        F.coalesce(F.col("we.duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)
