from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.year("event_time").alias("event_year"), "event_type")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
    .filter(F.col("avg_duration_sec") < 210)
    .select("event_year", "event_type", "avg_duration_sec")
    .orderBy("event_year")
)
