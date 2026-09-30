from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.session_id").isin([1, 5, 19, 35]))
    .select(
        F.col("s.device"),
        F.col("s.started_at"),
        F.col("we.page"),
        F.length(F.col("s.device")).alias("device_length"),
    )
)
