from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(~F.col("s.device").isin(["tablet", "desktop"]))
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("s.browser"),
        F.col("s.country"),
        F.col("s.started_at"),
    )
)
