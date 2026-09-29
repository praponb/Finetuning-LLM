from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("browser").isin(["Safari", "Firefox", "Chrome"])).alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("we.page"),
        F.col("we.event_time"),
    )
)
