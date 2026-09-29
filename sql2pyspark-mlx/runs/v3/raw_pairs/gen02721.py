from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") <= 311) & (F.col("duration_sec") == 443))
    .groupBy("event_type")
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)
