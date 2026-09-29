from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([5, 20, 21]) & F.hour("event_time").between(12, 19))
    .agg(F.countDistinct("duration_sec").alias("unique_event_types"))
    .orderBy("unique_event_types")
)
