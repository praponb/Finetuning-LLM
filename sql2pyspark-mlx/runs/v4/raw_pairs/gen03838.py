from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/home")
    .select("session_id", "duration_sec", "event_type")
    .orderBy(F.col("session_id").desc())
)
