from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([11, 19, 21, 30]))
    .groupBy("event_type")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.min("event_time").alias("first_event_time"),
        F.count("*").alias("num_web_events"),
    )
)
