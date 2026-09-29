from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 1)
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(F.countDistinct("duration_sec").alias("unique_event_types"))
    .filter(F.col("unique_event_types") < 8)
    .orderBy("unique_event_types")
)
