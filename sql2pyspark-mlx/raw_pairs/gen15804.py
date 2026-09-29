from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.min("event_time").alias("min_event_time"),
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("*").alias("num_web_events"),
    )
    .filter(F.col("num_web_events") > 3)
    .orderBy("min_event_time", "page")
    .limit(50)
)
