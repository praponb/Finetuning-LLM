from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter((F.col("we.page") == "/search") & (F.col("s.started_at") >= "2024-04-01"))
    .select(
        F.col("s.device"),
        F.col("we.page"),
        F.col("we.duration_sec"),
        F.col("we.event_type"),
        F.concat_ws("-", F.col("s.browser"), F.col("s.country")).alias("browser_country_key"),
    )
)
