from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(218, 327))
    .cube("session_id", F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)
