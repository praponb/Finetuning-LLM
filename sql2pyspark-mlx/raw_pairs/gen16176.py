from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .select(
        F.col("we.duration_sec"),
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.concat(F.col("s.browser"), F.lit("/"), F.col("s.country")).alias("browser_country"),
    )
)
