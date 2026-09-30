from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([17, 18, 33, 35]))
    .select("id", "event_type", "session_id")
    .orderBy(F.col("id").desc())
)
