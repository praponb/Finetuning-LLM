from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([7, 19]) & F.col("session_id").isin([1, 3, 22, 33]))
    .cube("event_type", F.to_date("event_time").alias("event_day"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
    .orderBy("event_type", "avg_duration_sec")
)
