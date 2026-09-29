from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNotNull() & (F.to_date("event_time") >= "2023-08-01") & F.col("session_id").isin([2, 9, 21]))
    .groupBy("page")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.max("event_time").alias("last_event_time"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("latest_event_time")
)
