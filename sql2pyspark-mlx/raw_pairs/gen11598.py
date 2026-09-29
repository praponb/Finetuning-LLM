from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .filter(F.col("total_duration_sec") < 1300)
    .drop("total_duration_sec")
    .orderBy("event_month")
)
