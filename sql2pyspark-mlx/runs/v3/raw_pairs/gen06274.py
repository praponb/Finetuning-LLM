from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([22, 30, 40]))
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.min("event_time").alias("min_event_time"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
    .orderBy("min_event_time", "event_month")
    .limit(5)
)
