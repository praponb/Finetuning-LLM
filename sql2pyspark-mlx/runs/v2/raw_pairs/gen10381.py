from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .select(
        F.col("we.page"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("s.customer_id"),
        F.substring(F.col("s.browser"), 1, 4).alias("browser_prefix"),
    )
)
