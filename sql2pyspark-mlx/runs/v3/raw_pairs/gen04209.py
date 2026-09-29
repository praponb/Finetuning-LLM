from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(8, 15) & F.col("session_id").isin([1, 8, 20, 36]))
    .select("id", "page", "event_type", "event_time")
    .orderBy("event_time", "id")
    .limit(5)
)
