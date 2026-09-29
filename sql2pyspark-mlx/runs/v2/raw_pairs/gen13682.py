from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.page").isNull())
    .select(
        F.col("we.session_id"),
        F.col("we.page"),
        F.col("s.started_at"),
        F.col("we.duration_sec").cast("double").alias("duration_sec_num"),
    )
    .orderBy(F.col("started_at").desc())
)
