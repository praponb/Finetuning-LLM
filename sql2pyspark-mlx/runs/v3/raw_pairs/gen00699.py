from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.page") == "/home")
    .select(
        F.col("we.page"),
        F.col("we.duration_sec"),
        F.col("s.started_at"),
        F.round((F.col("we.duration_sec") * 1.15), 2).alias("duration_sec_scaled"),
    )
)
