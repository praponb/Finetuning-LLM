from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter((F.col("s.device") == "desktop") & (F.col("s.device") == "desktop"))
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.session_id"),
        F.col("s.started_at"),
        F.upper(F.col("s.browser")).alias("browser_upper"),
    )
    .orderBy("browser_upper")
)
