from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page", "event_type")
    .agg(
        F.count("*").alias("num_web_events"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)
