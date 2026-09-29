from pyspark.sql import functions as F

result = (
    df_web_events
    .select("event_time", F.hour("event_time").alias("event_time_hour"))
    .orderBy("event_time")
)
