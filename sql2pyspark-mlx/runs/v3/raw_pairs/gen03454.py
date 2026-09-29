from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(7, 13) & (F.col("event_time") < "2025-08-19"))
    .select("event_type", "duration_sec", F.hour("event_time").alias("event_time_hour"))
    .orderBy("event_type")
)
