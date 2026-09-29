from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.started_at") < "2023-03-11")
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("we.page"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("s.browser"),
    )
    .orderBy(F.col("started_at").desc(), "id")
    .limit(5)
)
