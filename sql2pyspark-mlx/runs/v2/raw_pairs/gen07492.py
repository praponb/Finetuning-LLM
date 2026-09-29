from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.min("event_time").alias("min_event_time"),
    )
    .filter(F.col("duration_sec_count") >= 5)
    .select("event_month", "duration_sec_count", "min_event_time")
    .orderBy(F.col("event_month").desc())
    .limit(20)
)
