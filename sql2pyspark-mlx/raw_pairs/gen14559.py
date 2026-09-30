from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2023-09-01")
    .groupBy(F.hour("event_time").alias("event_hour"), "session_id")
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
)
