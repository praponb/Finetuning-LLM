from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter((F.col("we.page") == "/product") | F.col("s.country").isin(["SG", "UK"]))
    .select(F.col("we.id"), F.col("we.event_time"), F.col("s.device"))
    .orderBy("id")
    .limit(3)
)
