from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter((F.col("event_time") < "2023-06-01") & (F.hour("event_time").between(12, 17) | F.col("session_id").isin([1, 22, 23, 32])))
)

summary = (
    filtered_web_events
    .groupBy("event_type", "page")
    .agg(
        F.countDistinct("duration_sec").alias("unique_event_types"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = summary.filter(F.col("unique_event_types") >= 4)
