from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([14, 28, 33]))
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"), "event_type")
    .agg(F.countDistinct("duration_sec").alias("unique_sessions"))
    .filter(F.col("unique_sessions") >= 5)
    .orderBy("event_month", F.col("event_type").desc())
    .limit(20)
)
