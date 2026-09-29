from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") >= "2024-11-01")
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
    .filter(F.col("min_duration_sec") > 200)
    .select("event_month", "min_duration_sec", "duration_sec_range")
)
