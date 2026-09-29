from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)
