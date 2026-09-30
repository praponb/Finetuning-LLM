from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") < "2025-03-01")
    .agg(F.countDistinct("duration_sec").alias("unique_event_types"))
)
