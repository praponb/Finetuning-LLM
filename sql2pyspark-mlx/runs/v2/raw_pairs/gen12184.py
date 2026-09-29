from pyspark.sql import functions as F

result = (
    df_web_events
    .select("session_id", F.trim("event_type").alias("clean_event_type"))
    .orderBy("clean_event_type")
)
