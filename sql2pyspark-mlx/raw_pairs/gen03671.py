from pyspark.sql import functions as F

valid_web_events = df_web_events.filter(F.col("session_id") == 35)

event_type_summary = (
    valid_web_events
    .groupBy("event_type", F.month("event_time").alias("event_month"))
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

result = event_type_summary.filter(F.col("max_duration_sec") < 440)
