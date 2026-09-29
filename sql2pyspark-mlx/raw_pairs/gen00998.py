from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(12, 20) & (F.to_date("event_time") < "2025-09-01"))
    .select("page", "event_type", "duration_sec")
)
