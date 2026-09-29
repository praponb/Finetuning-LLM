from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter((F.col("we.session_id") == 26) & (F.col("s.device") == "desktop"))
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.page"),
        F.col("s.country"),
        F.col("s.browser"),
    )
)
