from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2024-01-01") & F.hour("event_time").between(10, 13))
    .select("page", "session_id")
)
