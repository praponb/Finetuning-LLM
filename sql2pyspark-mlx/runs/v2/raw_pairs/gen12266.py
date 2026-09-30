from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") >= "2024-12-01")
    .groupBy("page")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
    .filter(F.col("avg_duration_sec") < 460)
    .select("page", "avg_duration_sec")
    .orderBy("page")
)
