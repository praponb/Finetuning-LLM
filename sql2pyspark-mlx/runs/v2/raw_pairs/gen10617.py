from pyspark.sql import functions as F

result = (
    df_web_events
    .select("session_id", F.to_date("event_time").alias("event_time_date"))
    .orderBy("event_time_date")
)
