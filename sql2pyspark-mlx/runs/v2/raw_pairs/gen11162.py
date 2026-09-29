from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") != "click")
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
    .orderBy(F.col("stddev_duration_sec").desc())
)
