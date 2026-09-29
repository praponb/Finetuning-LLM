from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
    .filter(F.col("avg_duration_sec") >= 140)
    .select("event_month", "avg_duration_sec")
)
