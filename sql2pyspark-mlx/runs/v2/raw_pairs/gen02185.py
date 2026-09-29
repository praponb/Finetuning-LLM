from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([13, 21, 35, 38]))
    .select("id", "event_type")
    .orderBy("event_type", F.col("id").desc())
)
