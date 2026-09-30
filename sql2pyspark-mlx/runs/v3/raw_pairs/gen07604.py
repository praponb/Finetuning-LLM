from pyspark.sql import functions as F

result = (
    df_web_events
    .select("id", "event_type", "duration_sec")
    .orderBy("event_type", "id")
    .limit(10)
)
