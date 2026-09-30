from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(9, 13))
    .select("page", F.hour("event_time").alias("event_time_hour"))
)
