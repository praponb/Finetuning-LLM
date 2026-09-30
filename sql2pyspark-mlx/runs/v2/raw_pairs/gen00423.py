from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") != "click") & (F.to_date("event_time") < "2024-08-01"))
    .select("session_id", "event_time", "duration_sec")
)
