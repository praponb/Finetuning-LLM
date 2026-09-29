from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 35) & (F.col("event_time") < "2024-02-01"))
    .groupBy(F.year("event_time").alias("event_year"))
    .agg(F.countDistinct("duration_sec").alias("unique_event_types"))
)
