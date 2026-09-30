from pyspark.sql import functions as F

result = (
    df_web_events
    .select("event_type", F.hour("event_time").alias("event_time_hour"))
    .orderBy(F.col("event_time_hour").desc())
)
