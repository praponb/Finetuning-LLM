from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([14, 17, 39]))
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.min("event_time").alias("min_event_time"),
    )
    .filter(F.col("avg_duration_sec") >= 62)
    .orderBy(F.col("avg_duration_sec").desc(), "event_quarter")
    .limit(5)
)
