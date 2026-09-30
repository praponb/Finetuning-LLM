from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.country").isin(["TH", "JP"]))
    .select(F.col("s.device"), F.col("we.event_type"))
    .orderBy("device")
)
