from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(9, 12) & (F.col("event_time") < "2025-03-01"))
    .groupBy(
        "page",
        F.hour("event_time").alias("event_hour"),
        F.date_trunc("MONTH", "event_time").alias("event_month"),
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
    .filter(F.col("duration_sec_count") < 2)
    .drop("duration_sec_count")
    .orderBy(F.col("event_month").desc())
)
