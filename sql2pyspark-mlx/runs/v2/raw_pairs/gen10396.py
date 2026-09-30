from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(357, 375) & (F.to_date("event_time") >= "2024-05-01") & (F.col("page") == "/checkout"))
    .select("id", "duration_sec", "session_id", "event_time")
    .orderBy("id")
)
