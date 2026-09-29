from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .groupBy(F.col("s.browser"))
    .agg(
        (F.max(F.col("we.duration_sec")) - F.min(F.col("we.duration_sec"))).alias("duration_sec_range"),
    )
    .orderBy(F.col("duration_sec_range").desc())
)
