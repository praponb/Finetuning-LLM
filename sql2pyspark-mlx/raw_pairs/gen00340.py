from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("country").isin(["SG", "TH", "DE"])).alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .filter(F.col("we.event_time") >= "2023-09-10")
    .select(F.col("we.id"), F.col("we.event_time"))
)
