from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .filter(F.col("s.browser").isin(["Firefox", "Chrome"]) & F.col("s.country").isin(["DE", "US", "SG"]))
    .select(F.col("we.event_type"), F.col("s.browser"))
)
