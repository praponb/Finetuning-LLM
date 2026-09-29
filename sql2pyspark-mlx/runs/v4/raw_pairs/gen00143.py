from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.to_date("event_time") >= "2025-01-01").alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .filter(F.col("s.country").isin(["US", "TH", "DE"]))
    .select(F.col("s.id"), F.col("s.country"), F.col("s.device"), F.col("s.browser"))
)
